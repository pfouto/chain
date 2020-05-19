package frontend.notifications;

import babel.generic.ProtoNotification;

public class InstallSnapshotNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 103;

    private final byte[] state;

    public InstallSnapshotNotification(byte[] state) {
        super(NOTIFICATION_ID);
        this.state = state;
    }

    public byte[] getState() {
        return state;
    }
}
