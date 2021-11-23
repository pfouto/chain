package frontend.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import frontend.ops.OpBatch;

public class SubmitBatchRequest extends ProtoRequest {

    public static final short REQUEST_ID = 101;

    private final OpBatch batch;

    public SubmitBatchRequest(OpBatch batch) {
        super(REQUEST_ID);
        this.batch = batch;
    }

    public OpBatch getBatch() {
        return batch;
    }

}
