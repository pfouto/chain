package frontend.network;

import babel.generic.ProtoMessage;
import frontend.ops.OpBatch;
import frontend.ops.ReadOp;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class PeerReadResponseMessage extends ProtoMessage {

    public static final short MSG_CODE = 105;

    private final OpBatch responses;

    public PeerReadResponseMessage(OpBatch responses) {
        super(MSG_CODE);
        this.responses = responses;
    }

    public OpBatch getResponses() {
        return responses;
    }

    @Override
    public String toString() {
        return "PeerReadResponseMessage{" +
                "responses=" + responses +
                '}';
    }

    public static final ISerializer<PeerReadResponseMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PeerReadResponseMessage msg, ByteBuf out) throws IOException {
            OpBatch.serializer.serialize(msg.responses, out);
        }

        @Override
        public PeerReadResponseMessage deserialize(ByteBuf in) throws IOException {
            OpBatch responses = OpBatch.serializer.deserialize(in);
            return new PeerReadResponseMessage(responses);
        }
    };
}
