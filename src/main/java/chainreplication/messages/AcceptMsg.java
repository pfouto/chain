package chainreplication.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import common.values.PaxosValue;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.net.UnknownHostException;

public class AcceptMsg extends ProtoMessage {

    public static final short MSG_CODE = 302;

    public final int iN;
    public final PaxosValue value;

    public AcceptMsg(int iN, PaxosValue value) {
        super(MSG_CODE);
        this.iN = iN;
        this.value = value;
    }

    @Override
    public String toString() {
        return "AcceptMsg{" +
                "iN=" + iN +
                ", value=" + value +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptMsg>() {
        public void serialize(AcceptMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            PaxosValue.serializer.serialize(msg.value, out);
        }

        public AcceptMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            PaxosValue payload = PaxosValue.serializer.deserialize(in);
            return new AcceptMsg(instanceNumber, payload);
        }
    };
}
