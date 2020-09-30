package chainreplication.notifications;

import babel.generic.ProtoNotification;
import frontend.ops.OpBatch;

public class ReplyBatchNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 301;

    private final OpBatch batch;

    public ReplyBatchNotification(OpBatch batch) {
        super(NOTIFICATION_ID);
        this.batch = batch;
    }

    public OpBatch getBatch() {
        return batch;
    }
}
