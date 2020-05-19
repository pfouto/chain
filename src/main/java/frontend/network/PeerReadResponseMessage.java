package frontend.network;

import babel.generic.ProtoMessage;
import common.ReadOp;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class PeerReadResponseMessage extends ProtoMessage {

    public static final short MSG_CODE = 105;

    private final ReadOp op;

    public PeerReadResponseMessage(ReadOp op) {
        super(MSG_CODE);
        this.op = op;
    }

    public ReadOp getOp() {
        return op;
    }

    @Override
    public String toString() {
        return "PeerReadResponseMessage{" +
                "op=" + op +
                '}';
    }

    public static final ISerializer<PeerReadResponseMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PeerReadResponseMessage msg, ByteBuf out) {
            msg.op.serialize(out);
        }

        @Override
        public PeerReadResponseMessage deserialize(ByteBuf in) {
            return new PeerReadResponseMessage(ReadOp.deserialize(in));
        }
    };
}
