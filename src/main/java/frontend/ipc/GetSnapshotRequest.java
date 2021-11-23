package frontend.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class GetSnapshotRequest extends ProtoRequest {

    public static final short REQUEST_ID = 104;

    private final Host snapshotTarget;
    private final int snapshotInstance;

    public GetSnapshotRequest(Host snapshotTarget, int snapshotInstance) {
        super(REQUEST_ID);
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
