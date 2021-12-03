package chainpaxos.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.Queue;

public class ExecuteReadReply extends ProtoReply {

    public static final short REPLY_ID = 108;

    private final Queue<Long> batchIds;

    public ExecuteReadReply(Queue<Long> batchIds) {
        super(REPLY_ID);
        this.batchIds = batchIds;
    }

    public Queue<Long> getBatchIds() {
        return batchIds;
    }
}
