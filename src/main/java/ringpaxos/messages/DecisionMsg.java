package ringpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import ringpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class DecisionMsg extends ProtoMessage {

    public static final short MSG_CODE = 604;

    public final int iN;
    public final SeqN sN;

    public DecisionMsg(int iN, SeqN sN) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
    }

    @Override
    public String toString() {
        return "DecisionMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<DecisionMsg>() {
        public void serialize(DecisionMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
        }

        public DecisionMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            return new DecisionMsg(instanceNumber, sN);
        }
    };
}
