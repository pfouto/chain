package ringpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ReqAcceptMsg extends ProtoMessage {
    public static final short MSG_CODE = 607;
    public final int iN;

    public ReqAcceptMsg(int iN) {
        super(MSG_CODE);
        this.iN = iN;
    }

    @Override
    public String toString() {
        return "ReqAcceptMsg{" +
                "iN=" + iN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<ReqAcceptMsg>() {
        public void serialize(ReqAcceptMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
        }

        public ReqAcceptMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            return new ReqAcceptMsg(instanceNumber);
        }
    };
}
