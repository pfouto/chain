package chainpaxos;

import babel.exceptions.HandlerRegistrationException;
import channel.tcp.events.OutConnectionDown;
import channel.tcp.events.OutConnectionFailed;
import channel.tcp.events.OutConnectionUp;
import frontend.FrontendProto;
import frontend.ipc.SubmitBatchRequest;
import frontend.ipc.SubmitReadRequest;
import frontend.network.*;
import frontend.notifications.*;
import frontend.ops.OpBatch;
import frontend.ops.ReadOp;
import frontend.timers.BatchTimer;
import frontend.timers.InfoTimer;
import frontend.utils.OpInfo;
import io.netty.channel.EventLoopGroup;
import network.data.Host;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static frontend.network.RequestMessage.READ_STRONG;

public class ChainPaxosDelayedFront extends FrontendProto {

    private static final Logger logger = LogManager.getLogger(ChainPaxosDelayedFront.class);

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "ChainFrontDelay";

    public static final String READ_RESPONSE_BYTES_KEY = "read_response_bytes";
    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    public static final String LOCAL_BATCH_INTERVAL_KEY = "local_batch_interval";
    public static final String BATCH_SIZE_KEY = "batch_size";
    public static final String LOCAL_BATCH_SIZE_KEY = "local_batch_size";

    private final int BATCH_INTERVAL;
    private final int LOCAL_BATCH_INTERVAL;
    private final int BATCH_SIZE;
    private final int LOCAL_BATCH_SIZE;

    private Host writesTo;
    private long lastWriteBatchTime;
    private long lastReadBatchTime;


    //ToForward writes
    private List<OpInfo> writeInfoBuffer;
    private List<byte[]> writeDataBuffer;

    //ToSubmit reads
    private List<ReadOp> readBuffer;

    //Forwarded
    private final Queue<Triple<Long, List<OpInfo>, OpBatch>> pendingWrites;
    private final Queue<Pair<Long, List<ReadOp>>> pendingReads;

    private final byte[] response;

    public ChainPaxosDelayedFront(Properties props, EventLoopGroup workerGroup, short protoIndex) throws IOException {
        super(PROTOCOL_NAME_BASE + protoIndex, (short) (PROTOCOL_ID_BASE + protoIndex),
                props, workerGroup, protoIndex);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.LOCAL_BATCH_INTERVAL = Integer.parseInt(props.getProperty(LOCAL_BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));
        this.LOCAL_BATCH_SIZE = Integer.parseInt(props.getProperty(LOCAL_BATCH_SIZE_KEY));
        int READ_RESPONSE_BYTES = Integer.parseInt(props.getProperty(READ_RESPONSE_BYTES_KEY));

        response = new byte[READ_RESPONSE_BYTES];
        writesTo = null;
        writeInfoBuffer = new ArrayList<>(BATCH_SIZE);
        writeDataBuffer = new ArrayList<>(BATCH_SIZE);
        readBuffer = new ArrayList<>(LOCAL_BATCH_SIZE);

        pendingWrites = new LinkedList<>();
        pendingReads = new LinkedList<>();
    }

    @Override
    protected void _init(Properties props) throws HandlerRegistrationException {
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);
        setupPeriodicTimer(new BatchTimer(), LOCAL_BATCH_INTERVAL, LOCAL_BATCH_INTERVAL);

        setupPeriodicTimer(new InfoTimer(), 10000, 10000);
        registerTimerHandler(InfoTimer.TIMER_ID, this::debugInfo);

        lastWriteBatchTime = System.currentTimeMillis();
        lastReadBatchTime = System.currentTimeMillis();
    }

    /* -------------------- ---------- ----------------------------------------------- */
    /* -------------------- CLIENT OPS ----------------------------------------------- */
    /* -------------------- ---------- ----------------------------------------------- */
    @Override
    protected void onRequestMessage(RequestMessage msg, Host from, short sProto, int channel) {
        switch (msg.getOpType()) {
            case READ_STRONG:
                readBuffer.add(new ReadOp(msg.getOpId(), msg.getPayload(), from));
                if (readBuffer.size() == LOCAL_BATCH_SIZE)
                    sendNewReadBatch();
                break;
            case RequestMessage.WRITE:
                writeInfoBuffer.add(OpInfo.of(from, msg.getOpId(), msg.getOpType()));
                writeDataBuffer.add(msg.getPayload());

                if (writeInfoBuffer.size() == BATCH_SIZE)
                    sendNewWriteBatch();
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
        sendRequest(new SubmitBatchRequest(msg.getBatch()), ChainPaxosDelayedProto.PROTOCOL_ID);
    }

    /* -------------------- -------- ----------------------------------------------- */
    /* --------------------  TIMERS  ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    private void handleBatchTimer(BatchTimer timer, long l) {

        long currentTime = System.currentTimeMillis();

        //Send read buffer

        if (((lastReadBatchTime + LOCAL_BATCH_INTERVAL) < currentTime) && !readBuffer.isEmpty()) {
            logger.warn("Sending read batch by timeout, size " + readBuffer.size());
            if (readBuffer.size() > LOCAL_BATCH_SIZE)
                throw new IllegalStateException("Read batch too big " + readBuffer.size() + "/" + LOCAL_BATCH_SIZE);
            sendNewReadBatch();
        }

        //Check if write buffer timed out
        if (((lastWriteBatchTime + BATCH_INTERVAL) < currentTime) && !writeInfoBuffer.isEmpty()) {
            logger.warn("Sending write batch by timeout, size " + writeInfoBuffer.size());
            if (writeInfoBuffer.size() > BATCH_SIZE)
                throw new IllegalStateException("Write batch too big " + writeInfoBuffer.size() + "/" + BATCH_SIZE);

            sendNewWriteBatch();
        }
    }

    private void sendNewReadBatch() {
        long internalId = nextId();
        pendingReads.add(Pair.of(internalId, readBuffer));
        readBuffer = new ArrayList<>(LOCAL_BATCH_SIZE);
        sendRequest(new SubmitReadRequest(internalId, getProtoId()), ChainPaxosMixedProto.PROTOCOL_ID);
        lastReadBatchTime = System.currentTimeMillis();
    }

    private void sendNewWriteBatch() {
        long internalId = nextId();

        OpBatch batch = new OpBatch(internalId, self, getProtoId(), writeDataBuffer);
        pendingWrites.add(Triple.of(internalId, writeInfoBuffer, batch));

        writeInfoBuffer = new ArrayList<>(BATCH_SIZE);
        writeDataBuffer = new ArrayList<>(BATCH_SIZE);

        sendPeerWriteMessage(new PeerWriteMessage(batch), writesTo);
        lastWriteBatchTime = System.currentTimeMillis();
    }

    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.debug("Connected to writesTo " + event);
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
        if ((not.getBatch().getIssuer().equals(self)) && (not.getBatch().getFrontendId() == getProtoId())) {
            Triple<Long, List<OpInfo>, OpBatch> ops = pendingWrites.poll();
            if (ops == null || ops.getLeft() != not.getBatch().getBatchId()) {
                logger.error("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
                throw new AssertionError("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
            }
            ops.getMiddle().forEach(p -> sendMessage(serverChannel,
                    new ResponseMessage(p.getOpId(), p.getOpType(), new byte[0]), p.getClient()));
        }
    }

    protected void _onExecuteRead(ExecuteReadReply not, short from) {

        not.getBatchIds().forEach(bId -> {
            Pair<Long, List<ReadOp>> ops = pendingReads.poll();
            if (ops == null || !ops.getKey().equals(bId)) {
                logger.error("Expected " + bId + ". Got " + ops + "\n" +
                        pendingReads.stream().map(Pair::getKey).collect(Collectors.toList()));
                throw new AssertionError("Expected " + bId + ". Got " + ops);
            }
            ops.getRight().forEach(op -> sendMessage(serverChannel,
                    new ResponseMessage(op.getOpId(), READ_STRONG, response), op.getClient()));
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
