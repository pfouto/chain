package chainpaxos.messages;

import babel.generic.ProtoMessage;
import chainpaxos.values.MembershipOp;
import chainpaxos.values.PaxosValue;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class MembershipOpRequestMsg extends ProtoMessage {
    public static final short MSG_CODE = 206;

    public final MembershipOp op;

    public MembershipOpRequestMsg(MembershipOp op) {
        super(MSG_CODE);
        this.op = op;
    }

    @Override
    public String toString() {
        return "MembershipOpRequestMsg{" +
                "op=" + op +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<MembershipOpRequestMsg>() {
        public void serialize(MembershipOpRequestMsg msg, ByteBuf out) throws IOException {
            PaxosValue.serializer.serialize(msg.op, out);
        }

        public MembershipOpRequestMsg deserialize(ByteBuf in) throws IOException {
            MembershipOp deserialize = (MembershipOp) PaxosValue.serializer.deserialize(in);
            return new MembershipOpRequestMsg(deserialize);
        }

    };

}
