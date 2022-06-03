package distinguishedpaxos;

import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.babel.internal.MessageInEvent;
import pt.unl.fct.di.novasys.channel.tcp.MultithreadedTCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import common.values.AppOpBatch;
import common.values.NoOpValue;
import common.values.PaxosValue;
import distinguishedpaxos.messages.*;
import distinguishedpaxos.timers.LeaderTimer;
import distinguishedpaxos.timers.NoOpTimer;
import distinguishedpaxos.timers.ReconnectTimer;
import distinguishedpaxos.utils.AcceptedValue;
import distinguishedpaxos.utils.InstanceState;
import distinguishedpaxos.utils.Membership;
import distinguishedpaxos.utils.SeqN;
import frontend.notifications.ExecuteBatchNotification;
import frontend.notifications.MembershipChange;
import frontend.ipc.SubmitBatchRequest;
import frontend.timers.InfoTimer;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class MultiPaxosProto extends GenericProtocol {

    public final static short PROTOCOL_ID = 400;
    public final static String PROTOCOL_NAME = "MultiProto";
    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";
    public static final String QUORUM_SIZE_KEY = "quorum_size";
    public static final String LEADER_TIMEOUT_KEY = "leader_timeout";
    public static final String INITIAL_STATE_KEY = "initial_state";
    public static final String INITIAL_MEMBERSHIP_KEY = "initial_membership";
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    private static final Logger logger = LogManager.getLogger(MultiPaxosProto.class);
    private static final int INITIAL_MAP_SIZE = 1000;
    private final Map<Integer, InstanceState> instances = new HashMap<>(INITIAL_MAP_SIZE);
    private final int LEADER_TIMEOUT;
    private final int NOOP_SEND_INTERVAL;
    private final int QUORUM_SIZE;
    private final int RECONNECT_TIME;
    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    private final Host self;
    private final State state;
    private final LinkedList<Host> seeds;
    private final EventLoopGroup workerGroup;
    private Membership membership;
    private int highestAcceptedInstance = -1;
    private int highestDecidedInstance = -1;
    private int lastAcceptSent = -1;
    //Leadership
    private Map.Entry<Integer, SeqN> currentSN;
    private boolean amQuorumLeader;
    private long lastAcceptTime;
    //Timers
    private long noOpTimer = -1;
    private long lastLeaderOp;
    private int peerChannel;

    public MultiPaxosProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.workerGroup = workerGroup;

        currentSN = new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        amQuorumLeader = false;

        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));

        this.QUORUM_SIZE = Integer.parseInt(props.getProperty(QUORUM_SIZE_KEY));
        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));

        this.LEADER_TIMEOUT = Integer.parseInt(props.getProperty(LEADER_TIMEOUT_KEY));
        this.NOOP_SEND_INTERVAL = LEADER_TIMEOUT / 3;

        this.state = State.valueOf(props.getProperty(INITIAL_STATE_KEY));
        seeds = readSeeds(props.getProperty(INITIAL_MEMBERSHIP_KEY));
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        Properties peerProps = new Properties();
        peerProps.put(MultithreadedTCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        registerMessageSerializer(peerChannel, AcceptedMsg.MSG_CODE, AcceptedMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        registerMessageSerializer(peerChannel, DecidedMsg.MSG_CODE, DecidedMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareMsg.MSG_CODE, PrepareMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareOkMsg.MSG_CODE, PrepareOkMsg.serializer);

        registerMessageHandler(peerChannel, AcceptedMsg.MSG_CODE, this::uponAcceptedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, DecidedMsg.MSG_CODE, this::uponDecidedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareMsg.MSG_CODE, this::uponPrepareMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareOkMsg.MSG_CODE, this::uponPrepareOkMsg, this::uponMessageFailed);

        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

        registerTimerHandler(LeaderTimer.TIMER_ID, this::onLeaderTimer);
        registerTimerHandler(NoOpTimer.TIMER_ID, this::onNoOpTimer);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);

        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);

        if (state == State.ACTIVE) {
            if (!seeds.contains(self)) {
                logger.error("Non seed starting in active state\n" +
                        "Seeds raw: " + props.getProperty(INITIAL_MEMBERSHIP_KEY)+ "\n"+
                        "Seeds parsed: " + seeds + "\n" +
                        "Self: " + self);
                logger.error("Non seed starting in active state");
                throw new AssertionError("Non seed starting in active state");
            }
            membership = new Membership(seeds);
        } else { //never happens since there's only 1 possible state...
            logger.error("Invalid state: " + state);
            throw new AssertionError("Invalid state: " + state);
        }

        seeds.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);

        setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
        lastLeaderOp = System.currentTimeMillis();

        logger.info("MultiPaxos: " + membership + " qs " + QUORUM_SIZE);
    }

    private void onLeaderTimer(LeaderTimer timer, long timerId) {
        if (!amQuorumLeader && (System.currentTimeMillis() - lastLeaderOp > LEADER_TIMEOUT)) {
            tryTakeLeadership();
        }
    }

    private void onNoOpTimer(NoOpTimer timer, long timerId) {
        if (amQuorumLeader) {
            assert waitingAppOps.isEmpty();
            if (System.currentTimeMillis() - lastAcceptTime > NOOP_SEND_INTERVAL)
                sendNextAccept(new NoOpValue());
        } else {
            logger.warn(timer + " while not quorumLeader");
            cancelTimer(noOpTimer);
        }
    }

    private void tryTakeLeadership() { //Take leadership, send prepare
        logger.info("Attempting to take leadership...");
        assert !amQuorumLeader;
        InstanceState instance = instances.computeIfAbsent(highestDecidedInstance + 1, InstanceState::new);
        SeqN newSeqN = new SeqN(currentSN.getValue().getCounter() + 1, self);
        instance.prepareResponses.put(newSeqN, new HashSet<>());
        PrepareMsg pMsg = new PrepareMsg(instance.iN, newSeqN);
        membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
    }

    private void uponPrepareMsg(PrepareMsg msg, Host from, short sourceProto, int channel) {
        logger.debug(msg + " from:" + from);

        if (msg.iN > highestDecidedInstance) {
            assert msg.iN >= currentSN.getKey();
            if (!msg.sN.lesserOrEqualsThan(currentSN.getValue())) {
                //Change leader
                setNewInstanceLeader(msg.iN, msg.sN);

                //Gather list of accepts (if they exist)
                List<AcceptedValue> values = new ArrayList<>(Math.max(highestAcceptedInstance - msg.iN + 1, 0));
                for (int i = msg.iN; i <= highestAcceptedInstance; i++) {
                    InstanceState acceptedInstance = instances.get(i);
                    assert acceptedInstance.acceptedValue != null && acceptedInstance.highestAccept != null;
                    values.add(new AcceptedValue(i, acceptedInstance.highestAccept, acceptedInstance.acceptedValue));
                }
                sendOrEnqueue(new PrepareOkMsg(msg.iN, msg.sN, values), from);
                lastLeaderOp = System.currentTimeMillis();
            } else
                logger.warn("Discarding prepare since sN <= hP");
        } else { //Respond with decided message
            logger.debug("Responding with decided");
            List<AcceptedValue> values = new ArrayList<>(highestDecidedInstance - msg.iN + 1);
            for (int i = msg.iN; i <= highestDecidedInstance; i++) {
                InstanceState decidedInstance = instances.get(i);
                assert decidedInstance.isDecided();
                values.add(new AcceptedValue(i, decidedInstance.highestAccept, decidedInstance.acceptedValue));
            }
            sendOrEnqueue(new DecidedMsg(msg.iN, msg.sN, values), from);
        }
    }

    private void setNewInstanceLeader(int iN, SeqN sN) {
        assert iN >= currentSN.getKey();
        assert sN.greaterThan(currentSN.getValue());
        assert iN >= currentSN.getKey();

        currentSN = new AbstractMap.SimpleEntry<>(iN, sN);
        logger.info("New highest instance leader: iN:" + iN + ", " + sN);

        if (amQuorumLeader && !sN.getNode().equals(self)) {
            amQuorumLeader = false;
            cancelTimer(noOpTimer);
            waitingAppOps.clear();
        }
        triggerMembershipChangeNotification();
    }

    private void uponPrepareOkMsg(PrepareOkMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.get(msg.iN);
        logger.debug(msg + " from:" + from);
        if (instance == null || currentSN.getValue().greaterThan(msg.sN)) {
            logger.warn("Late prepareOk... ignoring");
            return;
        }

        Set<Host> okHosts = instance.prepareResponses.get(msg.sN);
        if (okHosts == null) {
            logger.warn("PrepareOk ignored, either already leader or stopped trying");
            return;
        }
        okHosts.add(from);

        //Update possible accepted values
        for (AcceptedValue acceptedValue : msg.acceptedValues) {
            InstanceState acceptedInstance = instances.computeIfAbsent(acceptedValue.instance, InstanceState::new);
            if (acceptedInstance.highestAccept == null || acceptedValue.sN.greaterThan(
                    acceptedInstance.highestAccept)) {
                acceptedInstance.forceAccept(acceptedValue.sN, acceptedValue.value);
                assert acceptedInstance.iN <= highestAcceptedInstance + 1;
                if (acceptedInstance.iN > highestAcceptedInstance) {
                    highestAcceptedInstance++;
                    assert acceptedInstance.iN == highestAcceptedInstance;
                }
            }
        }

        //Become leader
        if (okHosts.size() == Math.max(QUORUM_SIZE, membership.size() - QUORUM_SIZE + 1)) {
            instance.prepareResponses.remove(msg.sN);
            assert currentSN.getValue().equals(msg.sN);
            assert supportedLeader().equals(self);
            becomeLeader(msg.iN);
        }
    }

    private void becomeLeader(int instanceNumber) {
        amQuorumLeader = true;
        noOpTimer = setupPeriodicTimer(NoOpTimer.instance, NOOP_SEND_INTERVAL / 3, NOOP_SEND_INTERVAL / 3);
        logger.info("I am leader now! @ instance " + instanceNumber);

        //Propagate received accepted ops
        for (int i = instanceNumber; i <= highestAcceptedInstance; i++) {
            logger.debug("Propagating received operations: " + i);
            InstanceState aI = instances.get(i);
            assert aI.acceptedValue != null;
            assert aI.highestAccept != null;
            this.deliverMessageIn(new MessageInEvent(new BabelMessage(new AcceptMsg(i, currentSN.getValue(), aI.acceptedValue), (short) -1, (short) -1),
                    self, peerChannel));
        }
        lastAcceptSent = highestAcceptedInstance;

        PaxosValue nextOp;
        while ((nextOp = waitingAppOps.poll()) != null) {
            sendNextAccept(nextOp);
        }
    }

    private void uponDecidedMsg(DecidedMsg msg, Host from, short sourceProto, int channel) {
        logger.debug(msg + " from:" + from);

        InstanceState instance = instances.get(msg.iN);
        if (instance == null || msg.iN <= highestDecidedInstance || currentSN.getValue().greaterThan(msg.sN)) {
            logger.warn("Late decided... ignoring");
            return;
        }
        instance.prepareResponses.remove(msg.sN);
        //Update decided values
        for (AcceptedValue decidedValue : msg.decidedValues) {
            InstanceState decidedInst = instances.computeIfAbsent(decidedValue.instance, InstanceState::new);
            instance.registerPeerDecision(decidedValue.sN, decidedValue.value);
            if (!decidedInst.isDecided())
                maybeDecideAndExecute(decidedInst.iN);
        }

        //No-one tried to be leader after me, trying again
        if (currentSN.getValue().equals(msg.sN))
            tryTakeLeadership();
    }

    private void sendNextAccept(PaxosValue val) {
        assert supportedLeader().equals(self) && amQuorumLeader;

        InstanceState instance = instances.computeIfAbsent(Math.max(highestAcceptedInstance, lastAcceptSent) + 1,
                InstanceState::new);
        assert instance.acceptedValue == null && instance.highestAccept == null;

        PaxosValue nextValue;
        if (val.type == PaxosValue.Type.MEMBERSHIP) {
            logger.error("Membership op: " + val);
            throw new AssertionError("Membership op: " + val);
        } else if (val.type == PaxosValue.Type.APP_BATCH) {
            nextValue = val;
            //nBatches++;
            //nOpsBatched += ops.size();
        } else
            nextValue = new NoOpValue();
        AcceptMsg acceptMsg = new AcceptMsg(instance.iN, currentSN.getValue(), nextValue);
        membership.getMembers().stream().filter(h -> !h.equals(self)).forEach(h -> sendOrEnqueue(acceptMsg, h));
        uponAcceptMsg(acceptMsg, self, this.getProtoId(), peerChannel);
        lastAcceptSent = instance.iN;
        lastAcceptTime = System.currentTimeMillis();
    }

    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);
        lastLeaderOp = System.currentTimeMillis();

        if (instance.isDecided() && msg.sN.equals(instance.highestAccept)) {
            logger.debug("Discarding decided msg");
            return;
        }
        if (msg.sN.lesserThan(currentSN.getValue())) {
            logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        }
        if (msg.sN.greaterThan(currentSN.getValue()))
            setNewInstanceLeader(msg.iN, msg.sN);

        assert msg.sN.equals(currentSN.getValue());
        assert !instance.isDecided() || instance.acceptedValue.equals(msg.value);

        //Normal paxos - Accept
        instance.accept(msg.sN, msg.value);
        if (highestAcceptedInstance < instance.iN) {
            highestAcceptedInstance = instance.iN;
        }
        assert instance.acceptedValue != null;
        assert instance.highestAccept != null;

        if (!instance.isDecided() && instance.acceptedValue != null &&
                (instance.isPeerDecided() || instance.getAccepteds() >= QUORUM_SIZE)) { //We have quorum!
            maybeDecideAndExecute(instance.iN);
        }

        AcceptedMsg acceptedMsg = new AcceptedMsg(msg.iN, msg.sN);
        membership.getMembers().stream().filter(h -> !h.equals(self)).forEach(m -> sendOrEnqueue(acceptedMsg, m));
        uponAcceptedMsg(acceptedMsg, self, this.getProtoId(), peerChannel);
    }

    private void uponAcceptedMsg(AcceptedMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);
        if (msg.sN.lesserThan(currentSN.getValue())) {
            logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        }

        highestAcceptedInstance = Math.max(highestAcceptedInstance, instance.iN);

        int accepteds = instance.registerAccepted(msg.sN, from);
        if (!instance.isDecided() && instance.acceptedValue != null
                && (instance.isPeerDecided() || accepteds >= QUORUM_SIZE)) { //We have quorum!
            maybeDecideAndExecute(instance.iN);
        }
    }

    private void maybeDecideAndExecute(int instanceNumber) {
        if (instanceNumber != highestDecidedInstance + 1)
            return;

        InstanceState instance = instances.computeIfAbsent(highestDecidedInstance + 1, InstanceState::new);

        while (instance.acceptedValue != null &&  (instance.isPeerDecided() || instance.getAccepteds() >= QUORUM_SIZE)) {
            assert !instance.isDecided();
            decideAndExecute(instance);
            instance = instances.computeIfAbsent(highestDecidedInstance + 1, InstanceState::new);
        }
    }

    private void decideAndExecute(InstanceState instance) {
        assert instance.iN == 0 || instances.get(instance.iN - 1).isDecided();
        assert !instance.isDecided();
        instance.markDecided();
        highestDecidedInstance++;
        assert highestDecidedInstance == instance.iN;

        //Actually execute message
        logger.debug("Decided: " + instance.iN + " - " + instance.acceptedValue);
        if (instance.acceptedValue.type == PaxosValue.Type.APP_BATCH) {
            triggerNotification(new ExecuteBatchNotification(((AppOpBatch) instance.acceptedValue).getBatch()));
        } else if (instance.acceptedValue.type != PaxosValue.Type.NO_OP) {
            logger.error("Trying to execute unknown paxos value: " + instance.acceptedValue);
            throw new AssertionError("Trying to execute unknown paxos value: " + instance.acceptedValue);
        }
        //Resubmit timed out ops
    }

    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        logger.debug("Destination: " + destination);
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
        } else {
            if (destination.equals(self))
                deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short) -1, (short) -1), self, peerChannel));
            else sendMessage(msg, destination);
        }
    }

    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        if (amQuorumLeader)
            sendNextAccept(new AppOpBatch(not.getBatch()));
        else if (supportedLeader().equals(self))
            waitingAppOps.add(new AppOpBatch(not.getBatch()));
        else
            logger.warn("Received " + not + " without being leader, ignoring.");
    }

    private Host supportedLeader() {
        return currentSN.getValue().getNode();
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channel) {
        logger.warn(event);

        if (membership.contains(event.getNode())) {
            setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);
            if (supportedLeader().equals(event.getNode()))
                lastLeaderOp = 0;
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> ev, int ch) {
        logger.warn("Connection failed to " + ev.getNode() + ", cause: " + ev.getCause().getMessage());
        if (membership.contains(ev.getNode()))
            setupTimer(new ReconnectTimer(ev.getNode()), RECONNECT_TIME);
    }

    private void onReconnectTimer(ReconnectTimer timer, long timerId) {
        if (membership.contains(timer.getHost()))
            openConnection(timer.getHost());
    }

    private void uponInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void uponInConnectionDown(InConnectionDown event, int channel) {
        logger.info(event);
    }

    private void triggerMembershipChangeNotification() {
        triggerNotification(new MembershipChange(
                membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                null, supportedLeader().getAddress(), null));
    }

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    private LinkedList<Host> readSeeds(String membershipProp) throws UnknownHostException {
        LinkedList<Host> peers = new LinkedList<>();
        String[] initialMembership = membershipProp.split(",");
        for (String s : initialMembership) {
            peers.add(new Host(InetAddress.getByName(s), self.getPort()));
        }
        return peers;
    }

    enum State {ACTIVE}
}
