package common.notifications;

import babel.generic.ProtoNotification;
import common.WriteBatch;

public class SubmitBatchNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    private final WriteBatch batch;

    public SubmitBatchNotification(WriteBatch batch) {
        super(NOTIFICATION_ID);
        this.batch = batch;
    }

    public WriteBatch getBatch() {
        return batch;
    }
}
