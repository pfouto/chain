package chainreplication.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import frontend.ops.OpBatch;

public class ReplyBatchNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 301;

    private final OpBatch batch;
    private final long instId;

    public ReplyBatchNotification(OpBatch batch) {
        this(batch, -1);
    }
    public ReplyBatchNotification(OpBatch batch, long instId) {
        super(NOTIFICATION_ID);
        this.batch = batch;
        this.instId = instId;
    }

    public OpBatch getBatch() {
        return batch;
    }

    public long getInstId() {
        return instId;
    }
}
