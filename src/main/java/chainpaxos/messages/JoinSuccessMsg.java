package chainpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import chainpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinSuccessMsg extends ProtoMessage {

    public static final short MSG_CODE = 205;

    public final int iN;
    public final SeqN sN;
    public final List<Host> membership;

    public JoinSuccessMsg(int iN, SeqN sN, List<Host> membership) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.membership = membership;
    }

    @Override
    public String toString() {
        return "JoinSuccessMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", membership=" + membership +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<JoinSuccessMsg>() {
        public void serialize(JoinSuccessMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeInt(msg.membership.size());
            for (Host h : msg.membership)
                Host.serializer.serialize(h, out);
        }

        public JoinSuccessMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN seqN = SeqN.deserialize(in);
            int membershipSize = in.readInt();
            List<Host> membership = new ArrayList<>(membershipSize);
            for (int i = 0; i < membershipSize; i++)
                membership.add(Host.serializer.deserialize(in));
            return new JoinSuccessMsg(instanceNumber, seqN, membership);
        }
    };
}
