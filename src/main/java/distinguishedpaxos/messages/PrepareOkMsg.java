package distinguishedpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import distinguishedpaxos.utils.AcceptedValue;
import distinguishedpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PrepareOkMsg extends ProtoMessage {
    public static final short MSG_CODE = 408;
    public final int iN;
    public final SeqN sN;
    public final List<AcceptedValue> acceptedValues;

    public PrepareOkMsg(int iN, SeqN sN, List<AcceptedValue> acceptedValues) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.acceptedValues = acceptedValues;
    }

    @Override
    public String toString() {
        return "PrepareOkMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", acceptedValues=" + acceptedValues +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<PrepareOkMsg>() {
        public void serialize(PrepareOkMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeInt(msg.acceptedValues.size());
            for (AcceptedValue v : msg.acceptedValues) {
                v.serialize(out);
            }
        }

        public PrepareOkMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            int acceptedValuesLength = in.readInt();
            List<AcceptedValue> acceptedValues = new ArrayList<>(acceptedValuesLength);
            for (int i = 0; i < acceptedValuesLength; i++) {
                acceptedValues.add(AcceptedValue.deserialize(in));
            }
            return new PrepareOkMsg(instanceNumber, sN, acceptedValues);
        }
    };
}
