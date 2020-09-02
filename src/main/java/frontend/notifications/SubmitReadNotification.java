package frontend.notifications;

import babel.generic.ProtoNotification;
import frontend.ops.OpBatch;
import frontend.ops.ReadOp;

public class SubmitReadNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 107;

    private final ReadOp op;

    public SubmitReadNotification(ReadOp op) {
        super(NOTIFICATION_ID);
        this.op = op;
    }

    public ReadOp getOp() {
        return op;
    }
}
