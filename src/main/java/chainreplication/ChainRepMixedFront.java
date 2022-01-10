package chainreplication;

import app.Application;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import chainreplication.notifications.ReplyBatchNotification;
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
import java.util.concurrent.ConcurrentLinkedQueue;

public class ChainRepMixedFront extends FrontendProto {

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "ChainRepMixFront";
    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    public static final String BATCH_SIZE_KEY = "batch_size";
    private static final Logger logger = LogManager.getLogger(ChainRepMixedFront.class);
    private final int BATCH_INTERVAL;
    private final int BATCH_SIZE;
    //Forwarded
    private final Queue<Pair<Long, OpBatch>> pendingBatches;
    private Host writesTo;
    private long lastBatchTime;
    //ToForward
    private List<byte[]> opDataBuffer;


    public ChainRepMixedFront(Properties props, short protoIndex, Application app)
            throws IOException {
        super(PROTOCOL_NAME_BASE + protoIndex, (short) (PROTOCOL_ID_BASE + protoIndex),
                props, protoIndex, app);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));

        writesTo = null;
        opDataBuffer = new ArrayList<>(BATCH_SIZE);

        pendingBatches = new ConcurrentLinkedQueue<>();
    }

    @Override
    protected void _init(Properties props) throws HandlerRegistrationException {
        if(BATCH_SIZE > 1)
            setupPeriodicTimer(new BatchTimer(), BATCH_INTERVAL, (long) (BATCH_INTERVAL * 0.8));
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);

        subscribeNotification(ReplyBatchNotification.NOTIFICATION_ID, this::onReplyNotification);
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
        if(logger.isDebugEnabled()) logger.debug(msg + " from " + from);
        sendRequest(new SubmitBatchRequest(msg.getBatch()), ChainRepMixedProto.PROTOCOL_ID);
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
        pendingBatches.add(Pair.of(internalId, batch));
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
        pendingBatches.forEach(b -> sendPeerWriteMessage(new PeerBatchMessage(b.getRight()), writesTo));
    }

    private void sendPeerWriteMessage(PeerBatchMessage msg, Host destination) {
        if (destination.getAddress().equals(self)) onPeerBatchMessage(msg, destination, getProtoId(), peerChannel);
        else sendMessage(peerChannel, msg, destination);
    }

    /* -------------------- ------------- ----------------------------------------------- */
    /* -------------------- CONSENSUS OPS ----------------------------------------------- */
    /* -------------------- ------------- ----------------------------------------------- */
    protected void onExecuteBatch(ExecuteBatchNotification not, short from) {

    }

    protected void onReplyNotification(ReplyBatchNotification not, short from) {
        if ((not.getBatch().getIssuer().equals(self)) && (not.getBatch().getFrontendId() == getProtoId())) {
            Pair<Long, OpBatch> ops = pendingBatches.poll();
            if (ops == null || ops.getLeft() != not.getBatch().getBatchId()) {
                logger.error("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
                throw new AssertionError("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
            }
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, true, not.getInstId()));
        } else {
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
