package frontend.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

public class DeliverSnapshotReply extends ProtoReply {

    public static final short REPLY_ID = 106;

    private final Host snapshotTarget;
    private final int snapshotInstance;
    private final byte[] state;

    public DeliverSnapshotReply(Host snapshotTarget, int snapshotInstance, byte[] state) {
        super(REPLY_ID);
        this.snapshotTarget = snapshotTarget;
        this.snapshotInstance = snapshotInstance;
        this.state = state;
    }

    public byte[] getState() {
        return state;
    }

    public Host getSnapshotTarget() {
        return snapshotTarget;
    }

    public int getSnapshotInstance() {
        return snapshotInstance;
    }
}
