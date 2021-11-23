package distinguishedpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import common.values.PaxosValue;
import distinguishedpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class AcceptDecMsg extends ProtoMessage {

    public static final short MSG_CODE = 401;

    public final int iN;
    public final SeqN sN;
    public final PaxosValue value;

    public final List<DecisionMsg> decisionMsgs;

    public AcceptDecMsg(int iN, SeqN sN, PaxosValue value, List<DecisionMsg> decisionMsgs) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.value = value;
        this.decisionMsgs = decisionMsgs;
    }

    @Override
    public String toString() {
        return "AcceptMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", value=" + value +
                ", decMsgs=" + decisionMsgs +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptDecMsg>() {
        public void serialize(AcceptDecMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            PaxosValue.serializer.serialize(msg.value, out);
            ISerializer<DecisionMsg> serializer = (ISerializer<DecisionMsg>) DecisionMsg.serializer;
            out.writeInt(msg.decisionMsgs.size());
            for (DecisionMsg m : msg.decisionMsgs) {
                serializer.serialize(m, out);
            }
        }

        public AcceptDecMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            PaxosValue payload = PaxosValue.serializer.deserialize(in);
            int decSize = in.readInt();
            List<DecisionMsg> list = new LinkedList<>();
            for(int i = 0;i<decSize;i++){
                ProtoMessage deserialize = DecisionMsg.serializer.deserialize(in);
                list.add((DecisionMsg) deserialize);
            }
            return new AcceptDecMsg(instanceNumber, sN, payload, list);
        }
    };
}
