package chainpaxos;

import chainpaxos.ipc.ExecuteReadReply;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import chainpaxos.messages.*;
import chainpaxos.timers.*;
import chainpaxos.utils.AcceptedValue;
import chainpaxos.utils.InstanceState;
import chainpaxos.utils.Membership;
import chainpaxos.utils.SeqN;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.babel.internal.MessageInEvent;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import common.values.AppOpBatch;
import common.values.MembershipOp;
import common.values.NoOpValue;
import common.values.PaxosValue;
import frontend.ipc.DeliverSnapshotReply;
import frontend.ipc.GetSnapshotRequest;
import frontend.ipc.SubmitBatchRequest;
import chainpaxos.ipc.SubmitReadRequest;
import frontend.notifications.*;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class ChainPaxosDelayedProto extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(ChainPaxosDelayedProto.class);

    public final static short PROTOCOL_ID = 200;
    public final static String PROTOCOL_NAME = "ChainProtoDelay";

    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";
    public static final String QUORUM_SIZE_KEY = "quorum_size";
    public static final String LEADER_TIMEOUT_KEY = "leader_timeout";
    public static final String JOIN_TIMEOUT_KEY = "join_timeout";
    public static final String STATE_TRANSFER_TIMEOUT_KEY = "state_transfer_timeout";
    public static final String INITIAL_STATE_KEY = "initial_state";
    public static final String INITIAL_MEMBERSHIP_KEY = "initial_membership";
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    public static final String NOOP_INTERVAL_KEY = "noop_interval";

    private final int LEADER_TIMEOUT;
    private final int NOOP_SEND_INTERVAL;
    private final int QUORUM_SIZE;
    private final int RECONNECT_TIME;

    private final int STATE_TRANSFER_TIMEOUT;
    private final int JOIN_TIMEOUT;

    enum State {JOINING, WAITING_STATE_TRANSFER, ACTIVE}

    private final Set<Host> establishedConnections = new HashSet<>();

    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    private final Queue<MembershipOp> waitingMembershipOps = new LinkedList<>();

    private final Host self;
    private Host nextOk;
    private State state;
    private Membership membership;

    private static final int INITIAL_MAP_SIZE = 1000;
    private final Map<Integer, InstanceState> instances = new HashMap<>(INITIAL_MAP_SIZE);

    private int highestAcknowledgedInstance = -1;
    private int highestAcceptedInstance = -1;
    private int highestDecidedInstance = -1;
    private int lastAcceptSent = -1;

    //Leadership
    private Map.Entry<Integer, SeqN> currentSN;
    private boolean amQuorumLeader;
    private long lastAcceptTime;

    //Timers
    private long joinTimer = -1;
    private long stateTransferTimer = -1;
    private long noOpTimer = -1;

    private long lastLeaderOp;

    //Dynamic membership
    //TODO eventually forget stored states... (irrelevant for experiments)
    //Waiting for application to generate snapshot (boolean represents if we should send as soon as ready)
    private final Map<Host, MutablePair<Integer, Boolean>> pendingSnapshots = new HashMap<>();
    //Application-generated snapshots
    private final Map<Host, Pair<Integer, byte[]>> storedSnapshots = new HashMap<>();
    //List of JoinSuccess (nodes who generated snapshots for me)
    private final Queue<Host> hostsWithSnapshot = new LinkedList<>();

    private final LinkedList<Host> seeds;
    private int joiningInstance;
    private final Queue<AppOpBatch> bufferedOps = new LinkedList<>();
    private Map.Entry<Integer, byte[]> receivedState = null;

    private long leaderTimeoutTimer;

    private int peerChannel;

    private final EventLoopGroup workerGroup;

    public ChainPaxosDelayedProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID /*, new BetterEventPriorityQueue()*/);

        this.workerGroup = workerGroup;

        currentSN = new AbstractMap.SimpleEntry<>(-1, new SeqN(-1, null));
        amQuorumLeader = false;

        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));
        nextOk = null;

        this.QUORUM_SIZE = Integer.parseInt(props.getProperty(QUORUM_SIZE_KEY));
        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));

        this.LEADER_TIMEOUT = Integer.parseInt(props.getProperty(LEADER_TIMEOUT_KEY));
        if (props.containsKey(NOOP_INTERVAL_KEY))
            this.NOOP_SEND_INTERVAL = Integer.parseInt(props.getProperty(NOOP_INTERVAL_KEY));
        else
            this.NOOP_SEND_INTERVAL = LEADER_TIMEOUT / 3;

        this.JOIN_TIMEOUT = Integer.parseInt(props.getProperty(JOIN_TIMEOUT_KEY));
        this.STATE_TRANSFER_TIMEOUT = Integer.parseInt(props.getProperty(STATE_TRANSFER_TIMEOUT_KEY));

        this.state = State.valueOf(props.getProperty(INITIAL_STATE_KEY));
        seeds = readSeeds(props.getProperty(INITIAL_MEMBERSHIP_KEY));
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        Properties peerProps = new Properties();
        peerProps.put(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        registerMessageSerializer(peerChannel, AcceptAckMsg.MSG_CODE, AcceptAckMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        registerMessageSerializer(peerChannel, DecidedMsg.MSG_CODE, DecidedMsg.serializer);
        registerMessageSerializer(peerChannel, JoinRequestMsg.MSG_CODE, JoinRequestMsg.serializer);
        registerMessageSerializer(peerChannel, JoinSuccessMsg.MSG_CODE, JoinSuccessMsg.serializer);
        registerMessageSerializer(peerChannel, MembershipOpRequestMsg.MSG_CODE, MembershipOpRequestMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareMsg.MSG_CODE, PrepareMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareOkMsg.MSG_CODE, PrepareOkMsg.serializer);
        registerMessageSerializer(peerChannel, StateRequestMsg.MSG_CODE, StateRequestMsg.serializer);
        registerMessageSerializer(peerChannel, StateTransferMsg.MSG_CODE, StateTransferMsg.serializer);
        registerMessageSerializer(peerChannel, UnaffiliatedMsg.MSG_CODE, UnaffiliatedMsg.serializer);

        registerMessageHandler(peerChannel, UnaffiliatedMsg.MSG_CODE, this::uponUnaffiliatedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptAckMsg.MSG_CODE, this::uponAcceptAckMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, DecidedMsg.MSG_CODE, this::uponDecidedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, JoinRequestMsg.MSG_CODE,
                this::uponJoinRequestMsg, this::uponJoinRequestOut, this::uponMessageFailed);
        registerMessageHandler(peerChannel, JoinSuccessMsg.MSG_CODE,
                this::uponJoinSuccessMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, MembershipOpRequestMsg.MSG_CODE,
                this::uponMembershipOpRequestMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareMsg.MSG_CODE, this::uponPrepareMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareOkMsg.MSG_CODE, this::uponPrepareOkMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, StateRequestMsg.MSG_CODE,
                this::uponStateRequestMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, StateTransferMsg.MSG_CODE,
                this::uponStateTransferMsg, this::uponMessageFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

        registerTimerHandler(JoinTimer.TIMER_ID, this::onJoinTimer);
        registerTimerHandler(StateTransferTimer.TIMER_ID, this::onStateTransferTimer);
        registerTimerHandler(LeaderTimer.TIMER_ID, this::onLeaderTimer);
        registerTimerHandler(NoOpTimer.TIMER_ID, this::onNoOpTimer);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);

        registerReplyHandler(DeliverSnapshotReply.REPLY_ID, this::onDeliverSnapshot);
        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);
        registerRequestHandler(SubmitReadRequest.REQUEST_ID, this::onSubmitRead);

        if (state == State.ACTIVE) {
            if (!seeds.contains(self)) {
                logger.error("Non seed starting in active state");
                throw new AssertionError("Non seed starting in active state");
            }
            setupInitialState(seeds, -1);
        } else if (state == State.JOINING) {
            joinTimer = setupTimer(JoinTimer.instance, 1000);
        }

        logger.info("ChainProtoDelay: " + membership + " qs " + QUORUM_SIZE);
    }

    private void setupInitialState(List<Host> members, int instanceNumber) {
        membership = new Membership(members, QUORUM_SIZE);
        nextOk = membership.nextLivingInChain(self);
        members.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);
        joiningInstance = highestAcceptedInstance = highestAcknowledgedInstance = highestDecidedInstance =
                instanceNumber;
        leaderTimeoutTimer = setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
        lastLeaderOp = System.currentTimeMillis();
    }

    private void uponUnaffiliatedMsg(UnaffiliatedMsg msg, Host from, short sourceProto, int channel) {
        if (state == State.ACTIVE && membership.contains(from)){
            logger.error("Looks like I have unknowingly been removed from the membership, rejoining");
            instances.clear();
            cancelTimer(leaderTimeoutTimer);
            membership.getMembers().stream().filter(h -> !h.equals(self)).forEach(this::closeConnection);
            membership = null;
            state = State.JOINING;
            joinTimer = setupTimer(JoinTimer.instance, 1000);
        }
    }
    private void uponJoinRequestMsg(JoinRequestMsg msg, Host from, short sourceProto, int channel) {
        if (state == State.ACTIVE)
            if (supportedLeader() != null)
                sendOrEnqueue(new MembershipOpRequestMsg(MembershipOp.AddOp(from)), supportedLeader());
            else
                logger.warn("Nobody to re-propagate to");
        else
            logger.warn("Ignoring joinRequest while in " + state + " state: " + msg);
    }

    private void uponJoinSuccessMsg(JoinSuccessMsg msg, Host from, short sourceProto, int channel) {
        if (state == State.JOINING) {
            hostsWithSnapshot.add(from);
            cancelTimer(joinTimer);
            logger.info("Join successful");
            setupInitialState(msg.membership, msg.iN);
            setNewInstanceLeader(msg.iN, msg.sN);
            if (receivedState != null) {
                assert receivedState.getKey().equals(msg.iN);
                triggerNotification(new InstallSnapshotNotification(receivedState.getValue()));
                state = State.ACTIVE;
            } else {
                state = State.WAITING_STATE_TRANSFER;
                stateTransferTimer = setupTimer(StateTransferTimer.instance, STATE_TRANSFER_TIMEOUT);
            }
        } else if (state == State.WAITING_STATE_TRANSFER) {
            hostsWithSnapshot.add(from);
        } else
            logger.warn("Ignoring " + msg);
    }

    private void uponStateTransferMsg(StateTransferMsg msg, Host from, short sourceProto, int channel) {
        cancelTimer(stateTransferTimer);
        if (state == State.JOINING) {
            receivedState = new AbstractMap.SimpleEntry<>(msg.instanceNumber, msg.state);
        } else if (state == State.WAITING_STATE_TRANSFER) {
            assert msg.instanceNumber == joiningInstance;
            triggerNotification(new InstallSnapshotNotification(msg.state));
            state = State.ACTIVE;
            bufferedOps.forEach(o -> triggerNotification(new ExecuteBatchNotification(o.getBatch())));
        }
    }

    private void onStateTransferTimer(StateTransferTimer timer, long timerId) {
        if (state == State.WAITING_STATE_TRANSFER) {
            Host poll = hostsWithSnapshot.poll();
            if (poll == null) {
                logger.error("StateTransferTimeout and have nobody to ask a snapshot for...");
                throw new AssertionError();
            } else {
                sendMessage(new StateRequestMsg(joiningInstance), poll);
            }
        } else
            logger.warn("Unexpected StateTransferTimer");
    }

    private void onJoinTimer(JoinTimer timer, long timerId) {
        if (state == State.JOINING) {
            Optional<Host> seed = seeds.stream().filter(h -> !h.equals(self)).findAny();
            if (seed.isPresent()) {
                openConnection(seed.get());
                sendMessage(new JoinRequestMsg(), seed.get());
                logger.info("Sending join msg to: " + seed.get());
                joinTimer = setupTimer(JoinTimer.instance, JOIN_TIMEOUT);
            } else {
                throw new IllegalStateException("No seeds to communicate with...");
            }
        } else
            logger.warn("Unexpected JoinTimer");
    }

    private void uponJoinRequestOut(JoinRequestMsg msg, Host to, short destProto, int channelId) {
        closeConnection(to);
    }


    private void uponStateRequestMsg(StateRequestMsg msg, Host from, short sourceProto, int channel) {
        Pair<Integer, byte[]> storedState = storedSnapshots.get(from);
        if (storedState != null) {
            sendMessage(new StateTransferMsg(storedState.getKey(), storedState.getValue()), from);
        } else if (pendingSnapshots.containsKey(from)) {
            pendingSnapshots.get(from).setRight(true);
        } else {
            logger.error("Received stateRequest without having a pending snapshot... " + msg);
            throw new AssertionError("Received stateRequest without having a pending snapshot... " + msg);
        }
    }

    public void onDeliverSnapshot(DeliverSnapshotReply not, short from) {
        MutablePair<Integer, Boolean> pending = pendingSnapshots.remove(not.getSnapshotTarget());
        assert not.getSnapshotInstance() == pending.getLeft();
        storedSnapshots.put(not.getSnapshotTarget(), Pair.of(not.getSnapshotInstance(), not.getState()));
        if (pending.getRight()) {
            sendMessage(new StateTransferMsg(not.getSnapshotInstance(), not.getState()), not.getSnapshotTarget());
        }
    }

    private void onLeaderTimer(LeaderTimer timer, long timerId) {
        if (!amQuorumLeader && (System.currentTimeMillis() - lastLeaderOp > LEADER_TIMEOUT) &&
                (supportedLeader() == null
                        //Not required, avoids joining nodes from ending in the first half of
                        // the chain and slowing everything down. Only would happen with leader
                        // election simultaneous with nodes joining.
                        || membership.distanceFrom(self, supportedLeader()) <= membership.size() - QUORUM_SIZE)) {
            tryTakeLeadership();
        }
    }

    private void onNoOpTimer(NoOpTimer timer, long timerId) {
        if (amQuorumLeader) {
            assert waitingAppOps.isEmpty() && waitingMembershipOps.isEmpty();
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
        InstanceState instance = instances.computeIfAbsent(highestAcknowledgedInstance + 1, InstanceState::new);
        SeqN newSeqN = new SeqN(currentSN.getValue().getCounter() + 1, self);
        instance.prepareResponses.put(newSeqN, new HashSet<>());
        PrepareMsg pMsg = new PrepareMsg(instance.iN, newSeqN);
        membership.getMembers().forEach(h -> sendOrEnqueue(pMsg, h));
    }

    private void uponPrepareMsg(PrepareMsg msg, Host from, short sourceProto, int channel) {
        logger.debug(msg + " : " + from);
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }

        if (msg.iN > highestAcknowledgedInstance) {

            assert msg.iN >= currentSN.getKey();
            if (!msg.sN.lesserOrEqualsThan(currentSN.getValue())) {
                //Accept - Change leader
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
            logger.info("Responding with decided");
            List<AcceptedValue> values = new ArrayList<>(highestDecidedInstance - msg.iN + 1);
            for (int i = msg.iN; i <= highestDecidedInstance; i++) {
                InstanceState decidedInstance = instances.get(i);
                assert decidedInstance.isDecided();
                values.add(new AcceptedValue(i, decidedInstance.highestAccept, decidedInstance.acceptedValue));
            }
            sendOrEnqueue(new DecidedMsg(msg.iN, msg.sN, values), from);
        }
    }

    private void uponPrepareOkMsg(PrepareOkMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.get(msg.iN);
        logger.debug(msg + " from:" + from);
        if (instance == null || msg.iN <= highestAcknowledgedInstance || currentSN.getValue().greaterThan(msg.sN)) {
            logger.warn("Late prepareOk... ignoring");
            return;
        }

        Set<Host> okHosts = instance.prepareResponses.get(msg.sN);
        if (okHosts == null) {
            logger.debug("PrepareOk ignored, either already leader or stopped trying");
            return;
        }
        okHosts.add(from);

        //Update possible accepted values
        for (AcceptedValue acceptedValue : msg.acceptedValues) {
            InstanceState acceptedInstance = instances.computeIfAbsent(acceptedValue.instance, InstanceState::new);
            if (acceptedInstance.highestAccept == null || acceptedValue.sN.greaterThan(
                    acceptedInstance.highestAccept)) {
                maybeCancelPendingRemoval(instance.acceptedValue);
                acceptedInstance.forceAccept(acceptedValue.sN, acceptedValue.value);
                maybeAddToPendingRemoval(instance.acceptedValue);
                //Make sure there are no gaps between instances
                assert acceptedInstance.iN <= highestAcceptedInstance + 1;
                if (acceptedInstance.iN > highestAcceptedInstance) {
                    highestAcceptedInstance++;
                    assert acceptedInstance.iN == highestAcceptedInstance;
                }
            }
        }

        //Become leader
        //TODO maybe only need the second argument of max?
        if (okHosts.size() == Math.max(QUORUM_SIZE, membership.size() - QUORUM_SIZE + 1)) {
            instance.prepareResponses.remove(msg.sN);
            assert currentSN.getValue().equals(msg.sN);
            assert supportedLeader().equals(self);
            becomeLeader(msg.iN);
        }
    }

    private void becomeLeader(int instanceNumber) {
        amQuorumLeader = true;
        noOpTimer = setupPeriodicTimer(NoOpTimer.instance, NOOP_SEND_INTERVAL, Math.max(NOOP_SEND_INTERVAL / 3,1));
        logger.info("I am leader now! @ instance " + instanceNumber);

        //Propagate received accepted ops
        for (int i = instanceNumber; i <= highestAcceptedInstance; i++) {
            if (logger.isDebugEnabled()) logger.debug("Propagating received operations: " + i);
            InstanceState aI = instances.get(i);
            assert aI.acceptedValue != null;
            assert aI.highestAccept != null;
            //goes to the end of the queue
            this.deliverMessageIn(new MessageInEvent(new BabelMessage(new AcceptMsg(i, currentSN.getValue(), (short) 0,
                    aI.acceptedValue, highestAcknowledgedInstance), (short)-1, (short)-1), self, peerChannel));
        }
        lastAcceptSent = highestAcceptedInstance;

        membership.getMembers().forEach(h -> {
            if (!h.equals(self) && !establishedConnections.contains(h)) {
                logger.info("Will propose remove " + h);
                uponMembershipOpRequestMsg(new MembershipOpRequestMsg(MembershipOp.RemoveOp(h)),
                        self, getProtoId(), peerChannel);
            }
        });

        lastAcceptTime = 0;

        PaxosValue nextOp;
        while ((nextOp = waitingMembershipOps.poll()) != null) {
            sendNextAccept(nextOp);
        }
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
            logger.debug("Deciding:" + decidedValue + ", have: " + instance);
            decidedInst.forceAccept(decidedValue.sN, decidedValue.value);
            //Make sure there are no gaps between instances
            assert instance.iN <= highestAcceptedInstance + 1;
            if (instance.iN > highestAcceptedInstance) {
                highestAcceptedInstance++;
                assert instance.iN == highestAcceptedInstance;
            }
            if (!decidedInst.isDecided())
                decideAndExecute(decidedInst);
        }
        //No-one tried to be leader after me, trying again
        if (currentSN.getValue().equals(msg.sN))
            tryTakeLeadership();
    }

    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        if(!membership.contains(from)){
            logger.warn("Received msg from unaffiliated host " + from);
            sendMessage(new UnaffiliatedMsg(), from, TCPChannel.CONNECTION_IN);
            return;
        }

        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);

        if (instance.isDecided() && msg.sN.equals(instance.highestAccept)) {
            logger.warn("Discarding decided msg");
            return;
        }
        if (msg.sN.lesserThan(currentSN.getValue())) {
            //logger.warn("Discarding accept since sN < hP: " + msg);
            return;
        }
        if (msg.sN.greaterThan(currentSN.getValue()))
            setNewInstanceLeader(msg.iN, msg.sN);

        if (msg.sN.equals(instance.highestAccept) && (msg.nodeCounter <= instance.counter)) {
            logger.warn("Discarding since same sN & leader, while counter <=");
            return;
        }

        lastLeaderOp = System.currentTimeMillis();

        assert msg.sN.equals(currentSN.getValue());

        maybeCancelPendingRemoval(instance.acceptedValue);
        instance.accept(msg.sN, msg.value, (short) (msg.nodeCounter + 1));
        if (highestAcceptedInstance < instance.iN) {
            highestAcceptedInstance++;
            assert highestAcceptedInstance == instance.iN;
        }

        if ((instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) &&
                (((MembershipOp) instance.acceptedValue).opType == MembershipOp.OpType.REMOVE))
            markForRemoval(instance);

        forward(instance);

        if (!instance.isDecided() && instance.counter >= QUORUM_SIZE) //We have quorum!
            decideAndExecute(instance);

        ackInstance(msg.ack);
    }

    private void markForRemoval(InstanceState inst) {
        MembershipOp op = (MembershipOp) inst.acceptedValue;
        membership.addToPendingRemoval(op.affectedHost);
        if (nextOk.equals(op.affectedHost)) {
            nextOk = membership.nextLivingInChain(self);
            for (int i = highestAcknowledgedInstance + 1; i < inst.iN; i++) {
                forward(instances.get(i));
            }
        }
    }

    private void forward(InstanceState inst) {
        //TODO some cases no longer happen (like leader being removed)
        if (!membership.contains(inst.highestAccept.getNode())) {
            logger.error("Received accept from removed node (?)");
            throw new AssertionError("Received accept from removed node (?)");
        }

        Iterator<Host> targets = membership.nextNodesUntil(self, nextOk);
        while (targets.hasNext()) {
            Host target = targets.next();
            if (target.equals(self)) {
                logger.error("Sending accept to myself: " + membership);
                throw new AssertionError("Sending accept to myself: " + membership);
            }

            if (membership.isAfterLeader(self, inst.highestAccept.getNode(), target)) { //am last
                assert target.equals(inst.highestAccept.getNode());
                //If last in chain than we must have decided (unless F+1 dead/inRemoval)
                if (inst.counter < QUORUM_SIZE) {
                    logger.error("Last living in chain cannot decide. Are f+1 nodes dead/inRemoval? "
                            + inst.counter);
                    throw new AssertionError("Last living in chain cannot decide. " +
                            "Are f+1 nodes dead/inRemoval? " + inst.counter);
                }
                sendMessage(new AcceptAckMsg(inst.iN), target);
            } else { //not last in chain...
                AcceptMsg msg = new AcceptMsg(inst.iN, inst.highestAccept, inst.counter, inst.acceptedValue,
                        highestAcknowledgedInstance);
                sendMessage(msg, target);
            }
        }
    }

    private void uponAcceptAckMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {
        //logger.debug(msg + " - " + from);
        if (msg.instanceNumber <= highestAcknowledgedInstance) {
            logger.warn("Ignoring acceptAck for old instance: " + msg);
            return;
        }

        //TODO never happens?
        InstanceState inst = instances.get(msg.instanceNumber);
        if (!amQuorumLeader || !inst.highestAccept.getNode().equals(self)) {
            logger.error("Received Ack without being leader...");
            throw new AssertionError("Received Ack without being leader...");
        }

        if (inst.acceptedValue.type != PaxosValue.Type.NO_OP)
            lastAcceptTime = 0; //Force sending a NO-OP (with the ack)

        ackInstance(msg.instanceNumber);
    }

    private void ackInstance(int instanceN) {
        //For nodes in the first half of the chain only
        for (int i = highestDecidedInstance + 1; i <= instanceN; i++) {
            InstanceState ins = instances.get(i);
            assert !ins.isDecided();
            decideAndExecute(ins);
            assert highestDecidedInstance == i;
        }

        //For everyone
        for (int i = highestAcknowledgedInstance + 1; i <= instanceN; i++) {
            InstanceState ins = instances.remove(i);
            ins.getAttachedReads().forEach((k, v) -> sendReply(new ExecuteReadReply(v, ins.iN), k));

            assert ins.isDecided();
            highestAcknowledgedInstance++;
            assert highestAcknowledgedInstance == i;
        }
    }

    private void uponMembershipOpRequestMsg(MembershipOpRequestMsg msg, Host from, short sourceProto, int channel) {
        if (amQuorumLeader)
            sendNextAccept(msg.op);
        else if (supportedLeader().equals(self))
            waitingMembershipOps.add(msg.op);
        else
            logger.warn("Received " + msg + " without being leader, ignoring.");
    }

    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        if (amQuorumLeader)
            sendNextAccept(new AppOpBatch(not.getBatch()));
        else if (supportedLeader().equals(self))
            waitingAppOps.add(new AppOpBatch(not.getBatch()));
        else
            logger.warn("Received " + not + " without being leader, ignoring.");
    }

    public void onSubmitRead(SubmitReadRequest not, short from) {
        int readInstance = highestAcceptedInstance + 1;
        instances.computeIfAbsent(readInstance, InstanceState::new).attachRead(not);
    }

    private void sendNextAccept(PaxosValue val) {
        assert supportedLeader().equals(self) && amQuorumLeader;

        InstanceState instance = instances.computeIfAbsent(lastAcceptSent + 1, InstanceState::new);
        assert instance.acceptedValue == null && instance.highestAccept == null;

        PaxosValue nextValue;
        if (val.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp next = (MembershipOp) val;
            if ((next.opType == MembershipOp.OpType.REMOVE && !membership.contains(next.affectedHost)) ||
                    (next.opType == MembershipOp.OpType.ADD && membership.contains(next.affectedHost))) {
                logger.warn("Duplicate or invalid mOp: " + next);
                nextValue = new NoOpValue();
            } else {
                if (next.opType == MembershipOp.OpType.ADD) //TODO remove this hack
                    next = MembershipOp.AddOp(next.affectedHost, membership.indexOf(self));
                nextValue = next;
            }

        } else if (val.type == PaxosValue.Type.APP_BATCH) {
            nextValue = val;
            //nBatches++;
            //nOpsBatched += ops.size();
        } else
            nextValue = new NoOpValue();

        this.uponAcceptMsg(new AcceptMsg(instance.iN, currentSN.getValue(),
                (short) 0, nextValue, highestAcknowledgedInstance), self, this.getProtoId(), peerChannel);

        lastAcceptSent = instance.iN;
        lastAcceptTime = System.currentTimeMillis();
    }

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        logger.debug(event);
        if(state == State.JOINING)
            return;
        if (membership.contains(event.getNode())) {
            establishedConnections.add(event.getNode());
            if (event.getNode().equals(nextOk))
                for (int i = highestAcceptedInstance; i <= highestAcknowledgedInstance && i >= 0; i++)
                    forward(instances.get(i));
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channel) {
        logger.warn(event);
        establishedConnections.remove(event.getNode());

        if (membership.contains(event.getNode())) {
            setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);

            if (amQuorumLeader)
                uponMembershipOpRequestMsg(new MembershipOpRequestMsg(MembershipOp.RemoveOp(event.getNode())),
                        self, getProtoId(), peerChannel);
            else if (supportedLeader().equals(event.getNode()))
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

    // ----------------------- Utils ------------------------------------

    private void decideAndExecute(InstanceState instance) {
        assert highestDecidedInstance == instance.iN - 1;
        assert !instance.isDecided();
        instance.markDecided();
        highestDecidedInstance++;

        //Actually execute message
        logger.debug("Decided: " + instance.iN + " - " + instance.acceptedValue);
        if (instance.acceptedValue.type == PaxosValue.Type.APP_BATCH) {
            if (state == State.ACTIVE)
                triggerNotification(new ExecuteBatchNotification(((AppOpBatch) instance.acceptedValue).getBatch()));
            else
                bufferedOps.add((AppOpBatch) instance.acceptedValue);
        } else if (instance.acceptedValue.type == PaxosValue.Type.MEMBERSHIP) {
            executeMembershipOp(instance);
        } else if (instance.acceptedValue.type != PaxosValue.Type.NO_OP) {
            logger.error("Trying to execute unknown paxos value: " + instance.acceptedValue);
            throw new AssertionError("Trying to execute unknown paxos value: " + instance.acceptedValue);
        }

    }

    private void executeMembershipOp(InstanceState instance) {
        MembershipOp o = (MembershipOp) instance.acceptedValue;
        Host target = o.affectedHost;
        if (o.opType == MembershipOp.OpType.REMOVE) {
            logger.info("Removed from membership: " + target + " in inst " + instance.iN);
            membership.removeMember(target);
            triggerMembershipChangeNotification();
            closeConnection(target);
        } else if (o.opType == MembershipOp.OpType.ADD) {
            logger.info("Added to membership: " + target + " in inst " + instance.iN);
            membership.addMember(target, o.position);
            triggerMembershipChangeNotification();
            nextOk = membership.nextLivingInChain(self);
            openConnection(target);

            if (state == State.ACTIVE) {
                sendMessage(new JoinSuccessMsg(instance.iN, instance.highestAccept, membership.shallowCopy()), target);
                assert highestDecidedInstance == instance.iN;
                //TODO need mechanism for joining node to inform nodes they can forget stored state
                pendingSnapshots.put(target, MutablePair.of(instance.iN, instance.counter == QUORUM_SIZE));
                sendRequest(new GetSnapshotRequest(target, instance.iN), ChainPaxosDelayedFront.PROTOCOL_ID_BASE);
            }
        }
    }

    private void maybeCancelPendingRemoval(PaxosValue value) {
        if (value != null && value.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp o = (MembershipOp) value;
            if (o.opType == MembershipOp.OpType.REMOVE)
                membership.cancelPendingRemoval(o.affectedHost);
        }
    }

    private void maybeAddToPendingRemoval(PaxosValue value) {
        if (value != null && value.type == PaxosValue.Type.MEMBERSHIP) {
            MembershipOp o = (MembershipOp) value;
            if (o.opType == MembershipOp.OpType.REMOVE)
                membership.addToPendingRemoval(o.affectedHost);
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
            waitingMembershipOps.clear();
        }
        triggerMembershipChangeNotification();
    }

    private Host supportedLeader() {
        return currentSN.getValue().getNode();
    }

    private LinkedList<Host> readSeeds(String membershipProp) throws UnknownHostException {
        LinkedList<Host> peers = new LinkedList<>();
        String[] initialMembership = membershipProp.split(",");
        for (String s : initialMembership) {
            peers.add(new Host(InetAddress.getByName(s), self.getPort()));
        }
        return peers;
    }

    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        logger.debug("Destination: " + destination);
        if (msg == null || destination == null) {
            logger.error("null: " + msg + " " + destination);
        } else {
            if (destination.equals(self)) deliverMessageIn(new MessageInEvent(new BabelMessage(msg, (short)-1, (short)-1), self, peerChannel));
            else sendMessage(msg, destination);
        }
    }

    private void triggerMembershipChangeNotification() {
        triggerNotification(new MembershipChange(
                membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                null, supportedLeader().getAddress(), null));
    }
}