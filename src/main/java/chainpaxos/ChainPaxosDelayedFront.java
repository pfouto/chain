package chainpaxos;

import app.Application;
import chainpaxos.ipc.ExecuteReadReply;
import chainpaxos.timers.ReconnectTimer;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import frontend.FrontendProto;
import frontend.ipc.SubmitBatchRequest;
import chainpaxos.ipc.SubmitReadRequest;
import frontend.network.*;
import frontend.notifications.*;
import frontend.ops.OpBatch;
import frontend.timers.BatchTimer;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class ChainPaxosDelayedFront extends FrontendProto {

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "ChainFrontDelay";
    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    public static final String LOCAL_BATCH_INTERVAL_KEY = "local_batch_interval";
    public static final String BATCH_SIZE_KEY = "batch_size";
    public static final String LOCAL_BATCH_SIZE_KEY = "local_batch_size";
    private static final Logger logger = LogManager.getLogger(ChainPaxosDelayedFront.class);
    private final int BATCH_INTERVAL;
    private final int LOCAL_BATCH_INTERVAL;
    private final int BATCH_SIZE;
    private final int LOCAL_BATCH_SIZE;
    //Forwarded
    private final Queue<Pair<Long, OpBatch>> pendingWrites;
    private final Queue<Pair<Long, List<byte[]>>> pendingReads;
    private final Object readLock = new Object();
    private final Object writeLock = new Object();
    private Host writesTo;
    private boolean writesToConnected;
    private long lastWriteBatchTime;
    private long lastReadBatchTime;
    //ToForward writes
    private List<byte[]> writeDataBuffer;
    //ToSubmit reads
    private List<byte[]> readDataBuffer;


    public ChainPaxosDelayedFront(Properties props, short protoIndex, Application app) throws IOException {
        super(PROTOCOL_NAME_BASE + protoIndex, (short) (PROTOCOL_ID_BASE + protoIndex),
                props, protoIndex, app);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.LOCAL_BATCH_INTERVAL = Integer.parseInt(props.getProperty(LOCAL_BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));
        this.LOCAL_BATCH_SIZE = Integer.parseInt(props.getProperty(LOCAL_BATCH_SIZE_KEY));

        writesTo = null;
        writesToConnected = false;
        writeDataBuffer = new ArrayList<>(BATCH_SIZE);
        readDataBuffer = new ArrayList<>(LOCAL_BATCH_SIZE);

        pendingWrites = new ConcurrentLinkedQueue<>();
        pendingReads = new ConcurrentLinkedQueue<>();
    }

    @Override
    protected void _init(Properties props) throws HandlerRegistrationException {
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);
        final int minTimer = Math.min(LOCAL_BATCH_INTERVAL, BATCH_INTERVAL);
        setupPeriodicTimer(new BatchTimer(), minTimer, minTimer);
        registerReplyHandler(ExecuteReadReply.REPLY_ID, this::onExecuteRead);

        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);

        lastWriteBatchTime = System.currentTimeMillis();
        lastReadBatchTime = System.currentTimeMillis();
    }

    /* -------------------- ---------- ----------------------------------------------- */
    /* -------------------- CLIENT OPS ----------------------------------------------- */
    /* -------------------- ---------- ----------------------------------------------- */
    @Override
    public void submitOperation(byte[] op, OpType type) {
        switch (type) {
            case STRONG_READ:
                synchronized (readLock) {
                    readDataBuffer.add(op);
                    if (readDataBuffer.size() == LOCAL_BATCH_SIZE)
                        sendNewReadBatch();
                }
                break;
            case WRITE:
                synchronized (writeLock) {
                    writeDataBuffer.add(op);
                    if (writeDataBuffer.size() == BATCH_SIZE)
                        sendNewWriteBatch();
                }
                break;
        }
    }

    /* -------------------- -------- ----------------------------------------------- */
    /* -------------------- PEER OPS ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    protected void onPeerBatchMessage(PeerBatchMessage msg, Host from, short sProto, int channel) {
        sendRequest(new SubmitBatchRequest(msg.getBatch()), ChainPaxosDelayedProto.PROTOCOL_ID);
    }

    /* -------------------- -------- ----------------------------------------------- */
    /* --------------------  TIMERS  ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    private void handleBatchTimer(BatchTimer timer, long l) {
        long currentTime = System.currentTimeMillis();
        //Send read buffer
        if ((lastReadBatchTime + LOCAL_BATCH_INTERVAL) < currentTime)
            synchronized (readLock) {
                if (!readDataBuffer.isEmpty()) {
                    logger.warn("Sending read batch by timeout, size " + readDataBuffer.size());
                    sendNewReadBatch();
                }
            }
        //Check if write buffer timed out
        if ((lastWriteBatchTime + BATCH_INTERVAL) < currentTime)
            synchronized (writeLock) {
                if (!writeDataBuffer.isEmpty()) {
                    logger.warn("Sending write batch by timeout, size " + writeDataBuffer.size());
                    sendNewWriteBatch();
                }
            }
    }

    private void sendNewReadBatch() {
        long internalId = nextId();
        pendingReads.add(Pair.of(internalId, readDataBuffer));
        readDataBuffer = new ArrayList<>(LOCAL_BATCH_SIZE);
        sendRequest(new SubmitReadRequest(internalId, getProtoId()), ChainPaxosMixedProto.PROTOCOL_ID);
        lastReadBatchTime = System.currentTimeMillis();
    }

    private void sendNewWriteBatch() {
        long internalId = nextId();
        OpBatch batch = new OpBatch(internalId, self, getProtoId(), writeDataBuffer);
        pendingWrites.add(Pair.of(internalId, batch));
        writeDataBuffer = new ArrayList<>(BATCH_SIZE);
        sendBatchToWritesTo(new PeerBatchMessage(batch));
        lastWriteBatchTime = System.currentTimeMillis();
    }

    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            writesToConnected = true;
            logger.debug("Connected to writesTo " + event);
            pendingWrites.forEach(b -> sendBatchToWritesTo(new PeerBatchMessage(b.getRight())));
        } else {
            logger.warn("Unexpected connectionUp, ignoring and closing: " + event);
            closeConnection(peer, peerChannel);
        }
    }

    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Lost connection to writesTo, re-connecting in 5: " + event);
            writesToConnected = false;
            setupTimer(new ReconnectTimer(writesTo), 5000);
        }
    }

    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Connection failed to writesTo, re-trying in 5: " + event);
            setupTimer(new ReconnectTimer(writesTo), 5000);
        } else {
            logger.warn("Unexpected connectionFailed, ignoring: " + event);
        }
    }

    private void onReconnectTimer(ReconnectTimer timer, long timerId) {
        if (timer.getHost().equals(writesTo)) {
            logger.info("Trying to reconnect to writesTo " + timer.getHost());
            openConnection(timer.getHost());
        }
    }

    private void sendBatchToWritesTo(PeerBatchMessage msg) {
        if (writesTo.getAddress().equals(self)) onPeerBatchMessage(msg, writesTo, getProtoId(), peerChannel);
        else if (writesToConnected) sendMessage(peerChannel, msg, writesTo);
    }

    /* -------------------- ------------- ----------------------------------------------- */
    /* -------------------- CONSENSUS OPS ----------------------------------------------- */
    /* -------------------- ------------- ----------------------------------------------- */
    protected void onExecuteBatch(ExecuteBatchNotification not, short from) {
        if ((not.getBatch().getIssuer().equals(self)) && (not.getBatch().getFrontendId() == getProtoId())) {
            Pair<Long, OpBatch> ops = pendingWrites.poll();
            if (ops == null || ops.getLeft() != not.getBatch().getBatchId()) {
                logger.error("Expected " + not.getBatch().getBatchId() + ". Got " + ops + "\n" +
                        pendingWrites.stream().map(Pair::getKey).collect(Collectors.toList()));
                throw new AssertionError("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
            }
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, true, not.getInstId()));
        } else {
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, false, not.getInstId()));
        }
    }

    protected void onExecuteRead(ExecuteReadReply not, short from) {
        not.getBatchIds().forEach(bId -> {
            Pair<Long, List<byte[]>> ops = pendingReads.poll();
            if (ops == null || !ops.getKey().equals(bId)) {
                logger.error("Expected " + bId + ". Got " + ops + "\n" +
                        pendingReads.stream().map(Pair::getKey).collect(Collectors.toList()));
                throw new AssertionError("Expected " + bId + ". Got " + ops);
            }
            ops.getRight().forEach(op -> app.executeOperation(op, true, not.getInstId()));
        });
    }

    protected void onMembershipChange(MembershipChange notification, short emitterId) {

        //update membership and responder
        membership = notification.getOrderedMembers();

        //Writes to changed
        if (writesTo == null || !notification.getWritesTo().equals(writesTo.getAddress())) {
            //Close old writesTo
            if (writesTo != null && !writesTo.getAddress().equals(self)) {
                writesToConnected = false;
                closeConnection(writesTo, peerChannel);
            }
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New writesTo: " + writesTo.getAddress());
            if (!writesTo.getAddress().equals(self))
                openConnection(writesTo, peerChannel);
        }
    }

}
