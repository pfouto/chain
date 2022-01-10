package uringpaxos;

import app.Application;
import frontend.FrontendProto;
import frontend.ipc.SubmitBatchRequest;
import frontend.network.PeerBatchMessage;
import frontend.notifications.ExecuteBatchNotification;
import frontend.notifications.MembershipChange;
import frontend.ops.OpBatch;
import frontend.timers.BatchTimer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class URingPaxosFront extends FrontendProto {

    public final static short PROTOCOL_ID_BASE = 100;
    public final static String PROTOCOL_NAME_BASE = "URingFront";
    public static final String BATCH_INTERVAL_KEY = "batch_interval";
    public static final String BATCH_SIZE_KEY = "batch_size";
    private static final Logger logger = LogManager.getLogger(URingPaxosFront.class);
    private final int BATCH_INTERVAL;
    private final int BATCH_SIZE;
    //Forwarded
    private final Queue<Pair<Long, OpBatch>> pendingBatches;
    private long lastBatchTime;
    //ToForward
    private List<byte[]> opDataBuffer;

    public URingPaxosFront(Properties props, short protoIndex, Application app) throws IOException {
        super(PROTOCOL_NAME_BASE + protoIndex, (short) (PROTOCOL_ID_BASE + protoIndex),
                props, protoIndex, app);

        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.BATCH_SIZE = Integer.parseInt(props.getProperty(BATCH_SIZE_KEY));

        opDataBuffer = new ArrayList<>(BATCH_SIZE);
        pendingBatches = new ConcurrentLinkedDeque<>();
    }

    @Override
    protected void _init(Properties props) throws HandlerRegistrationException {
        setupPeriodicTimer(new BatchTimer(), BATCH_INTERVAL, (long) (BATCH_INTERVAL * 0.8));
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);
        lastBatchTime = System.currentTimeMillis();
    }

    @Override
    public void submitOperation(byte[] op, OpType type) {
        synchronized (this) {
            opDataBuffer.add(op);
            if (opDataBuffer.size() == BATCH_SIZE)
                sendNewBatch();
        }
    }

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

        sendRequest(new SubmitBatchRequest(batch), URingPaxosProto.PROTOCOL_ID);
        lastBatchTime = System.currentTimeMillis();
    }

    protected void onPeerBatchMessage(PeerBatchMessage msg, Host from, short sProto, int channel) {
        throw new IllegalStateException();
    }

    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        throw new IllegalStateException();
    }

    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        throw new IllegalStateException();
    }

    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        throw new IllegalStateException();
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
        membership = notification.getOrderedMembers();
    }
}
