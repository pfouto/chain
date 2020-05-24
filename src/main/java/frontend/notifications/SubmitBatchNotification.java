package frontend.notifications;

import babel.generic.ProtoNotification;
import frontend.ops.OpBatch;

public class SubmitBatchNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    private final OpBatch batch;

    public SubmitBatchNotification(OpBatch batch) {
        super(NOTIFICATION_ID);
        this.batch = batch;
    }

    public OpBatch getBatch() {
        return batch;
    }
}
