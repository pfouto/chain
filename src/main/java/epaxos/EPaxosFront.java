package epaxos;

import app.Application;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import frontend.FrontendProto;
import frontend.ipc.SubmitBatchRequest;
import frontend.network.*;
import frontend.notifications.ExecuteBatchNotification;
import frontend.notifications.MembershipChange;
import frontend.ops.OpBatch;
import frontend.timers.BatchTimer;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class EPaxosFront extends FrontendProto {

    private static final Logger logger = LogManager.getLogger(EPaxosFront.class);

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "EPaxosFront";

    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    public static final String BATCH_SIZE_KEY = "batch_size";

    private final int BATCH_INTERVAL;
    private final int BATCH_SIZE;

    private Host writesTo;
    private long lastBatchTime;

    //ToForward
    private List<byte[]> opDataBuffer;

    //Forwarded
    private final ConcurrentMap<Long, OpBatch> pendingBatches;

    public EPaxosFront(Properties props, short protoIndex, Application app) throws IOException {
        super(PROTOCOL_NAME_BASE + protoIndex, (short) (PROTOCOL_ID_BASE + protoIndex),
                props, protoIndex, app);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));

        writesTo = null;
        opDataBuffer = new ArrayList<>(BATCH_SIZE);

        pendingBatches = new ConcurrentHashMap<>();
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
    public void submitOperation(byte[] op, OpType type) {
        synchronized (this) {
            opDataBuffer.add(op);
            if (opDataBuffer.size() == BATCH_SIZE)
                sendNewBatch();
        }
    }


    /* -------------------- -------- ----------------------------------------------- */
    /* -------------------- PEER OPS ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    protected void onPeerBatchMessage(PeerBatchMessage msg, Host from, short sProto, int channel) {
        sendRequest(new SubmitBatchRequest(msg.getBatch()), EPaxosProto.PROTOCOL_ID);
    }

    /* -------------------- -------- ----------------------------------------------- */
    /* --------------------  TIMERS  ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    private void handleBatchTimer(BatchTimer timer, long l) {
        if (((lastBatchTime + BATCH_INTERVAL) < System.currentTimeMillis())) {
            synchronized (this) {
                if (!opDataBuffer.isEmpty()) {
                    logger.warn("Sending batch by timeout, size " + opDataBuffer.size());
                    sendNewBatch();
                }
            }
        }
    }

    private void sendNewBatch() {
        long internalId = nextId();

        OpBatch batch = new OpBatch(internalId, self, getProtoId(), opDataBuffer);
        pendingBatches.put(internalId, batch);
        opDataBuffer = new ArrayList<>(BATCH_SIZE);
        sendPeerWriteMessage(new PeerBatchMessage(batch), writesTo);
        lastBatchTime = System.currentTimeMillis();
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
        pendingBatches.values().forEach(b -> sendPeerWriteMessage(new PeerBatchMessage(b), writesTo));
    }

    private void sendPeerWriteMessage(PeerBatchMessage msg, Host destination) {
        if (destination.getAddress().equals(self)) onPeerBatchMessage(msg, destination, getProtoId(), peerChannel);
        else sendMessage(peerChannel, msg, destination);
    }

    /* -------------------- ------------- ----------------------------------------------- */
    /* -------------------- CONSENSUS OPS ----------------------------------------------- */
    /* -------------------- ------------- ----------------------------------------------- */
    protected void onExecuteBatch(ExecuteBatchNotification not, short from) {
        if ((not.getBatch().getIssuer().equals(self)) && (not.getBatch().getFrontendId() == getProtoId())) {
            OpBatch ops = pendingBatches.remove(not.getBatch().getBatchId());
            if (ops == null) {
                logger.error("Expected " + not.getBatch().getBatchId() + ". Got " + null);
                throw new AssertionError("Expected " + not.getBatch().getBatchId() + ". Got " + null);
            }
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, true, not.getInstId()));
        }else {
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, false, not.getInstId()));
        }
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
