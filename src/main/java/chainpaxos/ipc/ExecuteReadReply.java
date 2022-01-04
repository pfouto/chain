package chainpaxos.ipc;

import frontend.ops.OpBatch;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.Queue;

public class ExecuteReadReply extends ProtoReply {

    public static final short REPLY_ID = 108;

    private final Queue<Long> batchIds;
    private final long instId;


    public ExecuteReadReply(Queue<Long> batchIds) {
        this(batchIds, -1);
    }

    public ExecuteReadReply(Queue<Long> batchIds, long instId) {
        super(REPLY_ID);
        this.batchIds = batchIds;
        this.instId = instId;
    }

    public Queue<Long> getBatchIds() {
        return batchIds;
    }

    public long getInstId() {
        return instId;
    }
}
