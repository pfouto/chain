package distinguishedpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import common.values.PaxosValue;
import distinguishedpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class AcceptMsg extends ProtoMessage {

    public static final short MSG_CODE = 402;

    public final int iN;
    public final SeqN sN;
    public final PaxosValue value;

    public AcceptMsg(int iN, SeqN sN, PaxosValue value) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.value = value;
    }

    @Override
    public String toString() {
        return "AcceptMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", value=" + value +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptMsg>() {
        public void serialize(AcceptMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            PaxosValue.serializer.serialize(msg.value, out);
        }

        public AcceptMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            PaxosValue payload = PaxosValue.serializer.deserialize(in);
            return new AcceptMsg(instanceNumber, sN, payload);
        }
    };
}
