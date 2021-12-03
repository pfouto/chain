package frontend.network;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import frontend.ops.OpBatch;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class PeerBatchMessage extends ProtoMessage {

    public static final short MSG_CODE = 103;

    private final OpBatch batch;

    public PeerBatchMessage(OpBatch batch) {
        super(MSG_CODE);
        this.batch = batch;
    }

    public OpBatch getBatch() {
        return batch;
    }

    @Override
    public String toString() {
        return "PeerWriteMessage{" +
                "batch=" + batch +
                '}';
    }

    public static final ISerializer<PeerBatchMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PeerBatchMessage peerBatchMessage, ByteBuf out) throws IOException {
            OpBatch.serializer.serialize(peerBatchMessage.batch, out);
        }

        @Override
        public PeerBatchMessage deserialize(ByteBuf in) throws IOException {
            OpBatch batch = OpBatch.serializer.deserialize(in);
            return new PeerBatchMessage(batch);
        }
    };
}
