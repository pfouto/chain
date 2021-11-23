package frontend.network;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import frontend.ops.OpBatch;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class PeerReadMessage extends ProtoMessage {

    public static final short MSG_CODE = 104;

    private final OpBatch batch;

    public PeerReadMessage(OpBatch batch) {
        super(MSG_CODE);
        this.batch = batch;
    }

    public OpBatch getBatch() {
        return batch;
    }

    @Override
    public String toString() {
        return "PeerReadMessage{" +
                "batch=" + batch +
                '}';
    }

    public static final ISerializer<PeerReadMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PeerReadMessage redirectWriteMessage, ByteBuf out) throws IOException {
            OpBatch.serializer.serialize(redirectWriteMessage.batch, out);
        }

        @Override
        public PeerReadMessage deserialize(ByteBuf in) throws IOException {
            OpBatch batch = OpBatch.serializer.deserialize(in);
            return new PeerReadMessage(batch);
        }
    };
}
