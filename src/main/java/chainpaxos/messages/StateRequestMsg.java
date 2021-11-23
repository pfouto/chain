package chainpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class StateRequestMsg extends ProtoMessage {

    public static final short MSG_CODE = 209;
    public final int instanceNumber;

    public StateRequestMsg(int instanceNumber) {
        super(MSG_CODE);
        this.instanceNumber = instanceNumber;
    }

    @Override
    public String toString() {
        return "StateRequestMsg{" +
                "instanceNumber=" + instanceNumber +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<StateRequestMsg>() {
        public void serialize(StateRequestMsg msg, ByteBuf out) {
            out.writeInt(msg.instanceNumber);
        }

        public StateRequestMsg deserialize(ByteBuf in) {
            int instanceNumber = in.readInt();
            return new StateRequestMsg(instanceNumber);
        }
    };
}
