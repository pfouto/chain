package chainpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class JoinRequestMsg extends ProtoMessage {

    public static final short MSG_CODE = 204;

    public JoinRequestMsg() {
        super(MSG_CODE);
    }

    @Override
    public String toString() {
        return "JoinRequestMsg{}";
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<JoinRequestMsg>() {
        public void serialize(JoinRequestMsg msg, ByteBuf out) {
        }

        public JoinRequestMsg deserialize(ByteBuf in) {
            return new JoinRequestMsg();
        }
    };
}
