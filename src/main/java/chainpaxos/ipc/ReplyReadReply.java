package chainpaxos.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.Queue;

public class ReplyReadReply extends ProtoReply {

    public static final short REPLY_ID = 201;

    private final Queue<Long> batchIds;

    public ReplyReadReply(Queue<Long> batchIds) {
        super(REPLY_ID);
        this.batchIds = batchIds;
    }

    public Queue<Long> getBatchIds() {
        return batchIds;
    }
}
