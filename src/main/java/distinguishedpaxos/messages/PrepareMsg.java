package distinguishedpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import distinguishedpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class PrepareMsg extends ProtoMessage {

    public static final short MSG_CODE = 407;

    public final int iN;
    public final SeqN sN;

    public PrepareMsg(int iN, SeqN sN) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
    }

    @Override
    public String toString() {
        return "PrepareMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<PrepareMsg>() {
        public void serialize(PrepareMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
        }

        public PrepareMsg deserialize(ByteBuf in) throws IOException {
            int iN = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            return new PrepareMsg(iN, sN);
        }
    };
}
