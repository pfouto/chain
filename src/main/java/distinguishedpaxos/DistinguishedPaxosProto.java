package distinguishedpaxos;

import babel.events.MessageInEvent;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.GenericProtocol;
import babel.generic.ProtoMessage;
import common.values.NoOpValue;
import common.values.PaxosValue;
import distinguishedpaxos.timers.*;
import channel.tcp.events.*;
import distinguishedpaxos.messages.*;
import channel.tcp.MultithreadedTCPChannel;
import common.values.AppOpBatch;
import distinguishedpaxos.messages.AcceptMsg;
import distinguishedpaxos.messages.DecidedMsg;
import distinguishedpaxos.messages.PrepareMsg;
import distinguishedpaxos.messages.PrepareOkMsg;
import distinguishedpaxos.timers.LeaderTimer;
import distinguishedpaxos.utils.AcceptedValue;
import distinguishedpaxos.utils.InstanceState;
import distinguishedpaxos.utils.Membership;
import distinguishedpaxos.utils.SeqN;
import frontend.notifications.ExecuteBatchNotification;
import frontend.notifications.MembershipChange;
import frontend.notifications.SubmitBatchNotification;
import io.netty.channel.EventLoopGroup;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class DistinguishedPaxosProto extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(DistinguishedPaxosProto.class);

    public final static short PROTOCOL_ID = 400;
    public final static String PROTOCOL_NAME = "DistPaxos";

    public static final String[] SUPPORTED_CONSISTENCIES = {"pcs", "serial"};

    private static final int INITIAL_MAP_SIZE = 1000;
    private final Map<Integer, InstanceState> instances = new HashMap<>(INITIAL_MAP_SIZE);

    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";
    public static final String QUORUM_SIZE_KEY = "quorum_size";
    public static final String LEADER_TIMEOUT_KEY = "leader_timeout";
    public static final String INITIAL_STATE_KEY = "initial_state";
    public static final String INITIAL_MEMBERSHIP_KEY = "initial_membership";
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    public static final String CONSISTENCY_KEY = "consistency";

    private final int LEADER_TIMEOUT;
    private final int NOOP_SEND_INTERVAL;
    private final int QUORUM_SIZE;
    private final int RECONNECT_TIME;

    private final String CONSISTENCY;

    enum State {ACTIVE}

    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();

    private final Host self;
    private final State state;
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

    private final LinkedList<Host> seeds;

    private final EventLoopGroup workerGroup;

    private int peerChannel;

    public DistinguishedPaxosProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
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

        this.CONSISTENCY = props.getProperty(CONSISTENCY_KEY);
        if (!Arrays.asList(SUPPORTED_CONSISTENCIES).contains(CONSISTENCY)) {
            logger.error("Unsupported consistency: " + CONSISTENCY);
            throw new AssertionError("Unsupported consistency: \" + CONSISTENCY");
        }

        this.state = State.valueOf(props.getProperty(INITIAL_STATE_KEY));
        seeds = readSeeds(props.getProperty(INITIAL_MEMBERSHIP_KEY));
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        Properties peerProps = new Properties();
        peerProps.put(MultithreadedTCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.put(MultithreadedTCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(MultithreadedTCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(MultithreadedTCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        registerMessageSerializer(AcceptedMsg.MSG_CODE, AcceptedMsg.serializer);
        registerMessageSerializer(AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        registerMessageSerializer(DecidedMsg.MSG_CODE, DecidedMsg.serializer);
        registerMessageSerializer(DecisionMsg.MSG_CODE, DecisionMsg.serializer);
        registerMessageSerializer(PrepareMsg.MSG_CODE, PrepareMsg.serializer);
        registerMessageSerializer(PrepareOkMsg.MSG_CODE, PrepareOkMsg.serializer);

        registerMessageHandler(peerChannel, AcceptedMsg.MSG_CODE, this::uponAcceptedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, DecidedMsg.MSG_CODE, this::uponDecidedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, DecisionMsg.MSG_CODE, this::uponDecisionMsg, this::uponMessageFailed);
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

        subscribeNotification(SubmitBatchNotification.NOTIFICATION_ID, this::onSubmitBatch);

        if (state == State.ACTIVE) {
            if (!seeds.contains(self)) {
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

        logger.info("DistinguishedPaxos: " + membership + " qs " + QUORUM_SIZE);
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
            this.deliverMessageIn(new MessageInEvent(new AcceptMsg(i, currentSN.getValue(), aI.acceptedValue),
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
        membership.getMembers().forEach(h -> sendOrEnqueue(acceptMsg, h));

        lastAcceptSent = instance.iN;
        lastAcceptTime = System.currentTimeMillis();
    }

    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);

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

        sendOrEnqueue(new AcceptedMsg(msg.iN, msg.sN, msg.value), from);
        lastLeaderOp = System.currentTimeMillis();
    }

    private void uponAcceptedMsg(AcceptedMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);
        if (msg.sN.lesserThan(currentSN.getValue())) {
            logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        }

        assert !instance.isDecided() || instance.acceptedValue.equals(msg.value);
        assert instance.highestAccept == null || msg.sN.greaterThan(instance.highestAccept)
                || instance.acceptedValue.equals(msg.value);

        highestAcceptedInstance = Math.max(highestAcceptedInstance, instance.iN);

        int accepteds = instance.registerAccepted(msg.sN, msg.value, from);
        if (!instance.isDecided() && (instance.isPeerDecided() || accepteds >= QUORUM_SIZE)) { //We have quorum!
            maybeDecideAndExecute(instance.iN);
        }
    }

    private void uponDecisionMsg(DecisionMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);

        if (instance.isDecided()) {
            assert instance.acceptedValue.equals(msg.value);
            //logger.warn("Ignoring msg: " + msg);
            //TODO eventually forget values (when receiving decision from everyone?) (irrelevant for experiments)
        } else {
            assert msg.sN.greaterThan(instance.highestAccept) || instance.acceptedValue.equals(msg.value);
            highestAcceptedInstance = Math.max(highestAcceptedInstance, instance.iN);
            instance.registerPeerDecision(msg.sN, msg.value);
            maybeDecideAndExecute(instance.iN);
        }
        instance.nodesDecided++;
        if (instance.nodesDecided == membership.size()) {
            instances.remove(msg.iN);
        }
        lastLeaderOp = System.currentTimeMillis();
    }

    private void maybeDecideAndExecute(int instanceNumber) {
        if (instanceNumber != highestDecidedInstance + 1)
            return;

        InstanceState instance = instances.computeIfAbsent(highestDecidedInstance + 1, InstanceState::new);

        while (instance.isPeerDecided() || instance.getAccepteds() >= QUORUM_SIZE) {
            assert !instance.isDecided();
            decideAndExecute(instance);
            if (instance.highestAccept.getNode().equals(self)) {
                DecisionMsg dMsg = new DecisionMsg(instance.iN, instance.highestAccept, instance.acceptedValue);
                membership.getMembers().stream().filter(h -> !h.equals(self)).forEach(h -> sendOrEnqueue(dMsg, h));
            }
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
    }

    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        logger.debug("Destination: " + destination);
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
        } else {
            if (destination.equals(self)) deliverMessageIn(new MessageInEvent(msg, self, peerChannel));
            else sendMessage(msg, destination);
        }
    }

    public void onSubmitBatch(SubmitBatchNotification not, short from) {
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

    private void triggerMembershipChangeNotification(){
        triggerNotification(new MembershipChange(
                membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                readsTo(),
                supportedLeader().getAddress(),
                null));
    }

    private InetAddress readsTo(){
        if(CONSISTENCY.equals("pcs")){
            return self.getAddress();
        } else if (CONSISTENCY.equals("serial")){
            return supportedLeader().getAddress();
        } else {
            logger.error("Unexpected consistency " + CONSISTENCY);
            throw new AssertionError("Unexpected consistency " + CONSISTENCY);
        }
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
}
