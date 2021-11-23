package chainreplication.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.Arrays;

public class StateTransferMsg extends ProtoMessage {

    public static final short MSG_CODE = 307;

    public final int instanceNumber;
    public final byte[] state;

    public StateTransferMsg(int instanceNumber, byte[] state) {
        super(MSG_CODE);
        this.instanceNumber = instanceNumber;
        this.state = state;
    }

    @Override
    public String toString() {
        return "StateTransferMsg{" +
                "instanceNumber=" + instanceNumber +
                ", state=" + Arrays.toString(state) +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<StateTransferMsg>() {
        public void serialize(StateTransferMsg msg, ByteBuf out) {
            out.writeInt(msg.instanceNumber);
            out.writeInt(msg.state.length);
            out.writeBytes(msg.state);
        }

        public StateTransferMsg deserialize(ByteBuf in) {
            int instanceNumber = in.readInt();
            int stateLength = in.readInt();
            byte[] state = new byte[stateLength];
            in.readBytes(state);
            return new StateTransferMsg(instanceNumber, state);
        }
    };
}
