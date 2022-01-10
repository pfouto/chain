package distinguishedpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import common.values.PaxosValue;
import distinguishedpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class AcceptedMsg extends ProtoMessage {

    public static final short MSG_CODE = 410;

    public final int iN;
    public final SeqN sN;

    public AcceptedMsg(int iN, SeqN sN) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
    }

    @Override
    public String toString() {
        return "AcceptedMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptedMsg>() {
        public void serialize(AcceptedMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
        }

        public AcceptedMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            return new AcceptedMsg(instanceNumber, sN);
        }
    };
}
