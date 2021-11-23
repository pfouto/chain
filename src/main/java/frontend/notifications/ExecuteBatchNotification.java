package frontend.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import frontend.ops.OpBatch;

public class ExecuteBatchNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 105;

    private final OpBatch batch;

    public ExecuteBatchNotification(OpBatch batch) {
        super(NOTIFICATION_ID);
        this.batch = batch;
    }

    public OpBatch getBatch() {
        return batch;
    }
}
