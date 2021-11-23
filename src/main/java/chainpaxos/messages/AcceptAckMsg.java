package chainpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptAckMsg extends ProtoMessage {

    public static final short MSG_CODE = 201;

    public final int instanceNumber;

    public AcceptAckMsg(int instanceNumber) {
        super(MSG_CODE);
        this.instanceNumber = instanceNumber;
    }

    @Override
    public String toString() {
        return "AcceptAckMsg{" +
                "instanceNumber=" + instanceNumber +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptAckMsg>() {
        public void serialize(AcceptAckMsg msg, ByteBuf out) {
            out.writeInt(msg.instanceNumber);
        }

        public AcceptAckMsg deserialize(ByteBuf in) {
            int instanceNumber = in.readInt();
            return new AcceptAckMsg(instanceNumber);
        }
    };
}
