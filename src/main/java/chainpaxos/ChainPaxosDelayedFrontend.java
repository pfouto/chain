package chainpaxos;

import babel.exceptions.HandlerRegistrationException;
import channel.tcp.events.OutConnectionDown;
import channel.tcp.events.OutConnectionFailed;
import channel.tcp.events.OutConnectionUp;
import frontend.FrontendProto;
import frontend.network.*;
import frontend.notifications.*;
import frontend.ops.OpBatch;
import frontend.ops.ReadOp;
import frontend.timers.BatchTimer;
import frontend.utils.OpInfo;
import io.netty.channel.EventLoopGroup;
import network.data.Host;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class ChainPaxosDelayedFrontend extends FrontendProto {

    private static final Logger logger = LogManager.getLogger(ChainPaxosDelayedFrontend.class);

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "ChainFrontend";

    public static final String READ_RESPONSE_BYTES_KEY = "read_response_bytes";
    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    public static final String BATCH_SIZE_KEY = "batch_size";

    private final int BATCH_INTERVAL;
    private final int BATCH_SIZE;

    private Host writesTo;
    private long lastBatchTime;


    //ToForward writes
    private List<OpInfo> opInfoBuffer;
    private List<byte[]> opDataBuffer;

    //Forwarded
    private final Queue<Triple<Long, List<OpInfo>, OpBatch>> pendingWrites;
    private final Queue<Pair<Long, OpInfo>> pendingReads;

    private final byte[] response;

    public ChainPaxosDelayedFrontend(Properties props, EventLoopGroup workerGroup) throws IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID, props, workerGroup);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));
        int READ_RESPONSE_BYTES = Integer.parseInt(props.getProperty(READ_RESPONSE_BYTES_KEY));

        response = new byte[READ_RESPONSE_BYTES];
        writesTo = null;
        opInfoBuffer = new ArrayList<>(BATCH_SIZE);
        opDataBuffer = new ArrayList<>(BATCH_SIZE);

        pendingWrites = new LinkedList<>();
        pendingReads = new LinkedList<>();
    }

    @Override
    protected void _init(Properties props) throws HandlerRegistrationException {
        setupPeriodicTimer(new BatchTimer(), BATCH_INTERVAL, (long) (BATCH_INTERVAL * 0.8));
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);

        lastBatchTime = System.currentTimeMillis();
    }

    /* -------------------- ---------- ----------------------------------------------- */
    /* -------------------- CLIENT OPS ----------------------------------------------- */
    /* -------------------- ---------- ----------------------------------------------- */
    @Override
    protected void onRequestMessage(RequestMessage msg, Host from, short sProto, int channel) {
        switch (msg.getOpType()) {
            case RequestMessage.READ_STRONG:
                long internalId = nextId();
                pendingReads.add(Pair.of(internalId, OpInfo.of(from, msg.getOpId(), msg.getOpType())));
                triggerNotification(new SubmitReadNotification(new ReadOp(internalId, msg.getPayload())));
                break;
            case RequestMessage.WRITE:
                opInfoBuffer.add(OpInfo.of(from, msg.getOpId(), msg.getOpType()));
                opDataBuffer.add(msg.getPayload());

                if (opInfoBuffer.size() == BATCH_SIZE)
                    sendNewBatch();
                break;
            case RequestMessage.READ_WEAK:
                sendMessage(serverChannel, new ResponseMessage(msg.getOpId(), msg.getOpType(), response), from);
                break;
        }
    }

    /* -------------------- -------- ----------------------------------------------- */
    /* -------------------- PEER OPS ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    protected void onPeerReadMessage(PeerReadMessage msg, Host from, short sProto, int channel) {
        throw new IllegalStateException();
    }

    protected void onPeerReadResponseMessage(PeerReadResponseMessage msg, Host from, short sProto, int channel) {
        throw new IllegalStateException();
    }

    protected void onPeerWriteResponseMessage(PeerWriteResponseMessage msg, Host from, short sProto, int channel) {
        throw new IllegalStateException();
    }

    protected void onPeerWriteMessage(PeerWriteMessage msg, Host from, short sProto, int channel) {
        triggerNotification(new SubmitBatchNotification(msg.getBatch()));
    }

    /* -------------------- -------- ----------------------------------------------- */
    /* --------------------  TIMERS  ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    private void handleBatchTimer(BatchTimer timer, long l) {

        long currentTime = System.currentTimeMillis();
        if (((lastBatchTime + BATCH_INTERVAL) < currentTime) && !opInfoBuffer.isEmpty()) {
            logger.warn("Sending batch by timeout, size " + opInfoBuffer.size());
            if (opInfoBuffer.size() > BATCH_SIZE)
                throw new IllegalStateException("Batch too big " + opInfoBuffer.size() + "/" + BATCH_SIZE);

            sendNewBatch();
        }
    }

    private void sendNewBatch() {
        long internalId = nextId();

        OpBatch batch = new OpBatch(internalId, self, opDataBuffer);
        pendingWrites.add(Triple.of(internalId, opInfoBuffer, batch));

        opInfoBuffer = new ArrayList<>(BATCH_SIZE);
        opDataBuffer = new ArrayList<>(BATCH_SIZE);

        sendPeerWriteMessage(new PeerWriteMessage(batch), writesTo);
        lastBatchTime = System.currentTimeMillis();
    }

    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.info("Connected to writesTo " + event);
        } else {
            logger.warn("Unexpected connectionUp, ignoring and closing: " + event);
            closeConnection(peer, peerChannel);
        }
    }

    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        //logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Lost connection to writesTo, re-connecting: " + event);
            connectAndSendPendingBatchesToWritesTo();
        }
    }

    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Connection failed to writesTo, re-trying: " + event);
            connectAndSendPendingBatchesToWritesTo();
        } else {
            logger.warn("Unexpected connectionFailed, ignoring: " + event);
        }
    }

    private void connectAndSendPendingBatchesToWritesTo() {
        if (!writesTo.getAddress().equals(self))
            openConnection(writesTo, peerChannel);
        pendingWrites.forEach(b -> sendPeerWriteMessage(new PeerWriteMessage(b.getRight()), writesTo));
    }

    private void sendPeerWriteMessage(PeerWriteMessage msg, Host destination) {
        if (destination.getAddress().equals(self)) onPeerWriteMessage(msg, destination, getProtoId(), peerChannel);
        else sendMessage(peerChannel, msg, destination);
    }

    /* -------------------- ------------- ----------------------------------------------- */
    /* -------------------- CONSENSUS OPS ----------------------------------------------- */
    /* -------------------- ------------- ----------------------------------------------- */
    protected void _onExecuteBatch(ExecuteBatchNotification not, short from) {
        if (not.getBatch().getIssuer().equals(self)) {
            Triple<Long, List<OpInfo>, OpBatch> ops = pendingWrites.poll();
            if (ops == null || ops.getLeft() != not.getBatch().getBatchId()) {
                logger.error("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
                throw new AssertionError("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
            }
            ops.getMiddle().forEach(p -> sendMessage(serverChannel,
                    new ResponseMessage(p.getOpId(), p.getOpType(), new byte[0]),
                    p.getClient()));
        }
    }

    protected void _onExecuteRead(ExecuteReadNotification not, short from) {
        not.getOps().forEach(op -> {
            Pair<Long, OpInfo> poll = pendingReads.poll();
            if (poll == null || poll.getKey() != op.getOpId()) {
                logger.error("Expected " + op.getOpId() + ". Got " + poll);
                throw new AssertionError("Expected " + op.getOpId() + ". Got " + poll);
            }
            sendMessage(serverChannel,
                    new ResponseMessage(poll.getRight().getOpId(), poll.getRight().getOpType(), response),
                    poll.getRight().getClient());
        });
    }

    protected void onMembershipChange(MembershipChange notification, short emitterId) {

        //update membership and responder
        membership = notification.getOrderedMembers();

        //Writes to changed
        if (writesTo == null || !notification.getWritesTo().equals(writesTo.getAddress())) {
            //Close old writesTo
            if (writesTo != null && !writesTo.getAddress().equals(self))
                closeConnection(writesTo, peerChannel);
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New writesTo: " + writesTo.getAddress());
            connectAndSendPendingBatchesToWritesTo();
        }
    }

}
