package uringpaxos.messages;

import common.values.PaxosValue;
import frontend.ops.OpBatch;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ProposalMsg extends ProtoMessage {
    public static final short MSG_CODE = 620;

    public final OpBatch value;

    public ProposalMsg(OpBatch value) {
        super(MSG_CODE);
        this.value = value;
    }

    @Override
    public String toString() {
        return "ProposalMsg{" +
                "value=" + value +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<ProposalMsg>() {
        public void serialize(ProposalMsg msg, ByteBuf out) throws IOException {
            OpBatch.serializer.serialize(msg.value, out);
        }

        public ProposalMsg deserialize(ByteBuf in) throws IOException {
            OpBatch deserialize = OpBatch.serializer.deserialize(in);
            return new ProposalMsg(deserialize);
        }
    };

}
