package chainpaxos;

import app.Application;
import chainpaxos.timers.ReconnectTimer;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.babel.handlers.NotificationHandler;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import frontend.FrontendProto;
import frontend.ipc.SubmitBatchRequest;
import frontend.network.*;
import frontend.notifications.*;
import frontend.ops.OpBatch;
import frontend.timers.BatchTimer;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ChainPaxosMixedFront extends FrontendProto {

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "ChainFrontMixed";
    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    public static final String BATCH_SIZE_KEY = "batch_size";
    private static final Logger logger = LogManager.getLogger(ChainPaxosMixedFront.class);
    private final int BATCH_INTERVAL;
    private final int BATCH_SIZE;
    //Forwarded
    private final Queue<Pair<Long, OpBatch>> pendingBatches;
    private long lastBatchTime;
    private Host writesTo;
    private boolean writesToConnected;
    //ToForward
    private List<byte[]> opDataBuffer;

    public ChainPaxosMixedFront(Properties props, short protoIndex, Application app) throws IOException {
        super(PROTOCOL_NAME_BASE + protoIndex, (short) (PROTOCOL_ID_BASE + protoIndex),
                props, protoIndex, app);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));

        writesTo = null;
        writesToConnected = false;

        opDataBuffer = new ArrayList<>(BATCH_SIZE);
        pendingBatches = new ConcurrentLinkedDeque<>();
    }

    @Override
    protected void _init(Properties props) throws HandlerRegistrationException {
        if(BATCH_SIZE > 1)
            setupPeriodicTimer(new BatchTimer(), BATCH_INTERVAL, (long) (BATCH_INTERVAL * 0.8));
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);

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
        sendRequest(new SubmitBatchRequest(msg.getBatch()), ChainPaxosMixedProto.PROTOCOL_ID);
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

        sendBatchToWritesTo(new PeerBatchMessage(batch));
        lastBatchTime = System.currentTimeMillis();
    }

    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            writesToConnected = true;
            logger.debug("Connected to writesTo " + event);
            pendingBatches.forEach(b -> sendBatchToWritesTo(new PeerBatchMessage(b.getRight())));
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
        //If mine, remove from pending
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
            if (writesTo != null && !writesTo.getAddress().equals(self)) {
                closeConnection(writesTo, peerChannel);
                writesToConnected = false;
            }
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New writesTo: " + writesTo.getAddress());
            if (!writesTo.getAddress().equals(self))
                openConnection(writesTo, peerChannel);
        }
    }
}
