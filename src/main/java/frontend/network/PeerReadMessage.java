package frontend.network;

import babel.generic.ProtoMessage;
import frontend.ops.ReadOp;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class PeerReadMessage extends ProtoMessage {

    public static final short MSG_CODE = 104;

    private final ReadOp op;

    public PeerReadMessage(ReadOp op) {
        super(MSG_CODE);
        this.op = op;
    }

    public ReadOp getOp() {
        return op;
    }

    @Override
    public String toString() {
        return "PeerReadMessage{" +
                "op=" + op +
                '}';
    }

    public static final ISerializer<PeerReadMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PeerReadMessage redirectWriteMessage, ByteBuf out) {
            redirectWriteMessage.op.serialize(out);
        }

        @Override
        public PeerReadMessage deserialize(ByteBuf in) {
            return new PeerReadMessage(ReadOp.deserialize(in));
        }
    };
}
