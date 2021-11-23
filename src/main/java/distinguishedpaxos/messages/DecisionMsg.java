package distinguishedpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import common.values.PaxosValue;
import distinguishedpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class DecisionMsg extends ProtoMessage {

    public static final short MSG_CODE = 411;

    public final int iN;
    public final SeqN sN;
    public final PaxosValue value;

    public DecisionMsg(int iN, SeqN sN, PaxosValue value) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.value = value;
    }

    @Override
    public String toString() {
        return "DecisionMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", value=" + value +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<DecisionMsg>() {
        public void serialize(DecisionMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            PaxosValue.serializer.serialize(msg.value, out);
        }

        public DecisionMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            PaxosValue payload = PaxosValue.serializer.deserialize(in);
            return new DecisionMsg(instanceNumber, sN, payload);
        }
    };
}
