package chainreplication;

import babel.events.MessageInEvent;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.GenericProtocol;
import babel.generic.ProtoMessage;
import chainreplication.messages.*;
import chainreplication.requests.MembershipChangeEvt;
import chainreplication.timer.JoinTimer;
import chainreplication.timer.ReconnectTimer;
import chainreplication.utils.Membership;
import chainreplication.zookeeper.IMembershipListener;
import chainreplication.zookeeper.ProcessNode;
import channel.tcp.MultithreadedTCPChannel;
import channel.tcp.events.*;
import common.values.AppOpBatch;
import frontend.notifications.*;
import io.netty.channel.EventLoopGroup;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ChainReplicationProto extends GenericProtocol implements IMembershipListener {

    private static final Logger logger = LogManager.getLogger(ChainReplicationProto.class);

    public final static short PROTOCOL_ID = 300;
    public final static String PROTOCOL_NAME = "ChainReplication";

    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";
    public static final String JOIN_TIMEOUT_KEY = "join_timeout";
    public static final String ZOOKEEPER_URL_KEY = "zookeeper_url";
    public static final String RECONNECT_TIME_KEY = "reconnect_time";


    private final int JOIN_TIMEOUT;
    private final int RECONNECT_TIME;
    private final String ZOOKEEPER_URL;

    enum State {JOINING, REGISTERING, ACTIVE}

    private final Host self;

    private int peerChannel;
    private final EventLoopGroup workerGroup;

    private State state;
    private Membership membership;
    private ProcessNode processNode;
    private int highestAcceptReceived = -1;
    private int highestAcceptSent = -1;

    private LinkedList<AcceptMsg> sent = new LinkedList<>();

    private Host pendingNewTail = null;

    //Timers
    private long joinTimer = -1;

    public ChainReplicationProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID /*, new BetterEventPriorityQueue()*/);

        this.workerGroup = workerGroup;
        this.self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));

        this.JOIN_TIMEOUT = Integer.parseInt(props.getProperty(JOIN_TIMEOUT_KEY));
        this.ZOOKEEPER_URL = props.getProperty(ZOOKEEPER_URL_KEY);
        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));

        this.membership = new Membership(-1, Collections.emptyList());
        this.state = State.JOINING;

    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        Properties peerProps = new Properties();
        peerProps.put(MultithreadedTCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.put(MultithreadedTCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(MultithreadedTCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(MultithreadedTCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        registerMessageSerializer(AcceptAckMsg.MSG_CODE, AcceptAckMsg.serializer);
        registerMessageSerializer(AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        registerMessageSerializer(JoinRequestMsg.MSG_CODE, JoinRequestMsg.serializer);
        registerMessageSerializer(StateTransferMsg.MSG_CODE, StateTransferMsg.serializer);

        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

        registerMessageHandler(peerChannel, AcceptAckMsg.MSG_CODE, this::uponAcceptAckMsg,
                this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg,
                this::uponMessageFailed);
        registerMessageHandler(peerChannel, JoinRequestMsg.MSG_CODE, this::uponJoinRequestMsg,
                this::uponMessageFailed);
        registerMessageHandler(peerChannel, StateTransferMsg.MSG_CODE, this::uponStateTransferMsg,
                this::uponMessageFailed);

        subscribeNotification(DeliverSnapshotNotification.NOTIFICATION_ID, this::onDeliverSnapshot);
        subscribeNotification(SubmitBatchNotification.NOTIFICATION_ID, this::onSubmitBatch);

        registerRequestHandler(MembershipChangeEvt.REQUEST_ID, this::onMembershipChangeEvt);

        registerTimerHandler(JoinTimer.TIMER_ID, this::onJoinTimer);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);

        joinTimer = setupTimer(JoinTimer.instance, 1000);

        final ExecutorService service = Executors.newSingleThreadExecutor();
        processNode = new ProcessNode(ZOOKEEPER_URL, self, this);
        final Future<?> status = service.submit(processNode);
        try {
            status.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Zookeeper error: " + e.getMessage() + " " + e);
            e.printStackTrace();
            service.shutdown();
            System.exit(1);
        }

        logger.info("Starting ChainReplication");
    }

    private void onJoinTimer(JoinTimer timer, long timerId) {
        if (state == State.JOINING) {
            if (membership.size() == 0) {
                joinTimer = setupTimer(JoinTimer.instance, JOIN_TIMEOUT / 10);
            } else {
                Host currentTail = processNode.getNodeAddress(membership.tailId());
                sendMessage(new JoinRequestMsg(), currentTail);
                logger.info("Sending join msg to: " + currentTail + " - " + membership.tailId());
                joinTimer = setupTimer(JoinTimer.instance, JOIN_TIMEOUT);
            }
        }
    }

    private void uponJoinRequestMsg(JoinRequestMsg msg, Host from, short sourceProto, int channel) {
        if (!membership.isTail()) {
            logger.warn("Not tail, ignoring..." + msg);
            return;
        }
        if (pendingNewTail != null) {
            logger.warn("Another new tail is pending, ignoring for now..." + msg);
            return;
        }
        // if(!pending.isEmpty()) throw new AssertionError();
        pendingNewTail = from;
        triggerNotification(new GetSnapshotNotification(pendingNewTail, highestAcceptReceived));
        openConnection(from);
    }

    public void onDeliverSnapshot(DeliverSnapshotNotification not, short from) {
        if (not.getSnapshotTarget().equals(pendingNewTail)) {
            StateTransferMsg sMsg = new StateTransferMsg(not.getSnapshotInstance(), not.getState());
            sendMessage(sMsg, pendingNewTail);
        } else {
            logger.error("Unexpected snapshot: " + not + " : " + pendingNewTail);
        }
    }

    private void uponStateTransferMsg(StateTransferMsg msg, Host from, short sourceProto, int channel) {
        if (state != State.JOINING) {
            logger.warn("Unexpected state transfer received in state " + state);
            throw new IllegalStateException();
        } else {
            logger.info("State received, registering self");
            triggerNotification(new InstallSnapshotNotification(msg.state));
            state = State.REGISTERING;
            try {
                processNode.registerSelf();
            } catch (IOException e) {
                logger.error("Error registering after state transfer: " + e.getMessage());
                throw new AssertionError("Error registering: " + e.getMessage());
            }
            highestAcceptReceived = msg.instanceNumber;
        }
    }

    public void onSubmitBatch(SubmitBatchNotification not, short from) {
        if (state == State.ACTIVE && membership.isHead())
            sendNextAccept(new AppOpBatch(not.getBatch()));
        else
            logger.warn("Received " + not + " without being leader, ignoring.");
    }

    private void sendNextAccept(AppOpBatch val) {
        sendMessage(new AcceptMsg(++highestAcceptSent, val), self);
    }

    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        if (msg.iN <= highestAcceptReceived) {
            logger.warn("Discarding duplicated msg " + msg);
            return;
        }

        if (msg.iN != highestAcceptReceived + 1) {
            logger.error("Skipped iN " + msg.iN + " " + highestAcceptReceived);
            throw new AssertionError("Skipped iN " + msg.iN + " " + highestAcceptReceived);
        }

        highestAcceptReceived = msg.iN;

        triggerNotification(new ExecuteBatchNotification(((AppOpBatch) msg.value).getBatch()));

        if (membership.isTail()) {
            if (pendingNewTail != null) {
                sent.add(msg);
            } else {
                //triggerNotification(new ExecuteBatchNotification(((AppOpBatch) msg.value).getBatch()));
                sendMessage(new AcceptAckMsg(msg.iN), processNode.getNodeAddress(membership.prevNode()));
            }
        } else {
            sent.add(msg);
            sendMessage(msg, processNode.getNodeAddress(membership.nextNode()));
        }
    }

    private void uponAcceptAckMsg(AcceptAckMsg msg, Host from, short sourceProto, int channel) {
        while (!sent.isEmpty() && sent.getFirst().iN <= msg.instanceNumber) {
            AcceptMsg toExec = sent.removeFirst();
            //triggerNotification(new ExecuteBatchNotification(((AppOpBatch) toExec.value).getBatch()));
        }

        if (!membership.isHead())
            sendMessage(msg, processNode.getNodeAddress(membership.prevNode()));
    }

    // Called from zookeeper thread
    @Override
    public void membershipChanged(int myId, List<Integer> l) {
        sendRequest(new MembershipChangeEvt(myId, l), getProtoId());
    }

    public void onMembershipChangeEvt(MembershipChangeEvt ev, int from) {
        Membership oldMembership = membership;
        membership = new Membership(ev.myId, ev.l);
        logger.info("New membership: " + membership);
        Set<Integer> addedNodes = new HashSet<>(membership.getMembers());
        addedNodes.removeAll(oldMembership.getMembers());
        addedNodes.forEach(id -> openConnection(processNode.getNodeAddress(id)));
        //if (!addedNodes.isEmpty()) logger.info("New nodes: " + addedNodes);

        Set<Integer> removedNodes = new HashSet<>(oldMembership.getMembers());
        removedNodes.removeAll(membership.getMembers());
        removedNodes.forEach(id -> closeConnection(processNode.getNodeAddress(id)));
        //if (!removedNodes.isEmpty()) logger.info("Removed nodes: " + removedNodes);

        if (state == State.JOINING) {
            //if(membership.contains(ev.myId) || ev.myId != -1) throw new AssertionError();
            if (membership.size() == 0) {
                logger.info("Looks like I'm the first one, registering myself directly");
                state = State.REGISTERING;
                try {
                    processNode.registerSelf();
                } catch (IOException e) {
                    logger.error("Error registering: " + e.getMessage());
                    throw new AssertionError("Error registering: " + e.getMessage());
                }
            }
        } else if (state == State.REGISTERING) {
            if (membership.contains(ev.myId)) {
                logger.info("Active! My id is: " + ev.myId);
                state = State.ACTIVE;
            }
        }

        if (state == State.ACTIVE) {

            if (!oldMembership.getMembers().isEmpty()) {
                //New head
                //if (membership.headId() != oldMembership.headId()) {
                //}

                //Prev node removed
                if (oldMembership.contains(ev.myId) && !oldMembership.isHead() &&
                        removedNodes.contains(oldMembership.prevNode())) {
                    logger.info("Registering sn of:" + highestAcceptReceived);
                    processNode.publishSN(oldMembership.prevNode(), highestAcceptReceived);
                }
                //Next node removed, and there is a next-next node to propagate missing accepts
                if (oldMembership.contains(ev.myId) && !oldMembership.isTail()
                        && removedNodes.contains(oldMembership.nextNode()) && !membership.isTail()) {
                    Integer sn = null;
                    logger.info("Looking for SN...");
                    //TODO something prettier below
                    //Can't do anything until new tail is active, so just block main thread
                    while (sn == null) {
                        sn = processNode.getSN(membership.nextNode(), oldMembership.nextNode());
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    final int finalSn = sn;
                    logger.info("Propagating messages starting from " + sn + " to " + membership.nextNode());
                    //if(membership.nextNode() == oldMembership.nextNode()) throw new AssertionError();
                    Host newNext = processNode.getNodeAddress(membership.nextNode());

                    sent.forEach(acceptMsg -> {
                        if (acceptMsg.iN > finalSn) sendMessage(acceptMsg, newNext);
                    });
                }
                if (oldMembership.isTail() && !membership.isTail()) {
                    Host newTail = processNode.getNodeAddress(membership.nextNode());
                    if (!newTail.equals(pendingNewTail)) {
                        logger.error("Tail error: " + newTail + " : " + pendingNewTail);
                        throw new IllegalStateException("Tail error: " + newTail + " : " + pendingNewTail);
                    }
                    pendingNewTail = null;
                    logger.info("Propagating 'sent' to " + newTail);
                    sent.forEach(acceptMsg -> sendMessage(acceptMsg, newTail));
                }
            }
            if (membership.isTail()) { //If was already tail, this does nothing, else executes all pending ops
                sent.forEach(acceptMsg -> sendOrEnqueue(new AcceptAckMsg(acceptMsg.iN), self));
            }

            if (membership.isHead())
                highestAcceptSent = highestAcceptReceived;

            triggerNotification(new MembershipChange(
                    membership.getMembers().stream().map(id -> processNode.getNodeAddress(id).getAddress())
                            .collect(Collectors.toList()),
                    self.getAddress(),
                    processNode.getNodeAddress(membership.headId()).getAddress(),
                    processNode.getNodeAddress(membership.tailId()).getAddress()));
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        logger.info(event);
        if (processNode.getAddresses(membership.getMembers()).contains(event.getNode())) {
            if (membership.isTail()) return;

            if (processNode.getNodeAddress(membership.nextNode()).equals(event.getNode()))
                sent.forEach(m -> sendMessage(m, event.getNode()));
        } else if (!pendingNewTail.equals(event.getNode()))
            closeConnection(event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channel) {
        logger.warn(event);

        if (processNode.getAddresses(membership.getMembers()).contains(event.getNode())) {
            setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> ev, int ch) {
        logger.warn("Connection failed to " + ev.getNode() + ", cause: " + ev.getCause().getMessage());
        if (processNode.getAddresses(membership.getMembers()).contains(ev.getNode()))
            setupTimer(new ReconnectTimer(ev.getNode()), RECONNECT_TIME);
    }

    private void onReconnectTimer(ReconnectTimer timer, long timerId) {
        if (processNode.getAddresses(membership.getMembers()).contains(timer.getHost()))
            openConnection(timer.getHost());
    }


    void sendOrEnqueue(ProtoMessage msg, Host destination) {
        logger.debug("Destination: " + destination);
        if (destination.equals(self)) deliverMessageIn(new MessageInEvent(msg, self, peerChannel));
        else sendMessage(msg, destination);
    }

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    private void uponInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void uponInConnectionDown(InConnectionDown event, int channel) {
        logger.info(event);
    }

}
