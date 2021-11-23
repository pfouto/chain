package ringpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ReqDecisionMsg extends ProtoMessage {
    public static final short MSG_CODE = 608;
    public final int iN;

    public ReqDecisionMsg(int iN) {
        super(MSG_CODE);
        this.iN = iN;
    }

    @Override
    public String toString() {
        return "ReqDecisionMsg{" +
                "iN=" + iN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<ReqDecisionMsg>() {
        public void serialize(ReqDecisionMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
        }

        public ReqDecisionMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            return new ReqDecisionMsg(instanceNumber);
        }
    };
}
