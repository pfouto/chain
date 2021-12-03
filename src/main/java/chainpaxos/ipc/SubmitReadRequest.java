package chainpaxos.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class SubmitReadRequest extends ProtoRequest {

    public static final short REQUEST_ID = 102;

    private final long batchId;
    private final short frontendId;

    public SubmitReadRequest(long batchId, short frontendId) {
        super(REQUEST_ID);
        this.batchId = batchId;
        this.frontendId = frontendId;
    }

    public long getBatchId() {
        return batchId;
    }

    public short getFrontendId() {
        return frontendId;
    }
}
