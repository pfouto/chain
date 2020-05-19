package frontend.notifications;

import babel.generic.ProtoNotification;
import network.data.Host;

public class GetSnapshotNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 104;

    private final Host snapshotTarget;
    private final int snapshotInstance;

    public GetSnapshotNotification(Host snapshotTarget, int snapshotInstance) {
        super(NOTIFICATION_ID);
        this.snapshotTarget = snapshotTarget;
        this.snapshotInstance = snapshotInstance;
    }

    public Host getSnapshotTarget() {
        return snapshotTarget;
    }

    public int getSnapshotInstance() {
        return snapshotInstance;
    }
}
