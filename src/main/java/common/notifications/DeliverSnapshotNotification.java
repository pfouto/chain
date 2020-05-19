package common.notifications;

import babel.generic.ProtoNotification;
import network.data.Host;

public class DeliverSnapshotNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 106;

    private final Host snapshotTarget;
    private final int snapshotInstance;
    private final byte[] state;

    public DeliverSnapshotNotification(Host snapshotTarget, int snapshotInstance, byte[] state) {
        super(NOTIFICATION_ID);
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
