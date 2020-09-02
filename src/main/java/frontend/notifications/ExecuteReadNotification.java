package frontend.notifications;

import babel.generic.ProtoNotification;
import frontend.ops.OpBatch;
import frontend.ops.ReadOp;

import java.util.Queue;

public class ExecuteReadNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 108;

    private final Queue<ReadOp> ops;

    public ExecuteReadNotification(Queue<ReadOp> ops) {
        super(NOTIFICATION_ID);
        this.ops = ops;
    }

    public Queue<ReadOp> getOps() {
        return ops;
    }
}
