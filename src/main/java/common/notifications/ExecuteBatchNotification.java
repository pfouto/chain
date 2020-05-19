package common.notifications;

import babel.generic.ProtoNotification;
import babel.generic.ProtoReply;
import babel.generic.ProtoRequest;
import common.WriteBatch;

public class ExecuteBatchNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 105;

    private final WriteBatch batch;

    public ExecuteBatchNotification(WriteBatch batch) {
        super(NOTIFICATION_ID);
        this.batch = batch;
    }

    public WriteBatch getBatch() {
        return batch;
    }
}
