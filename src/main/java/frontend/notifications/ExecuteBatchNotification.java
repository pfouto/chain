package frontend.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import frontend.ops.OpBatch;

public class ExecuteBatchNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 105;

    private final OpBatch batch;
    private final long instId;

    public ExecuteBatchNotification(OpBatch batch) {
        this(batch, -1);
    }

    public ExecuteBatchNotification(OpBatch batch, long instId) {
        super(NOTIFICATION_ID);
        this.instId = instId;
        this.batch = batch;
    }

    public OpBatch getBatch() {
        return batch;
    }

    public long getInstId() {
        return instId;
    }
}
