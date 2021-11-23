package chainpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import chainpaxos.utils.AcceptedValue;
import chainpaxos.utils.SeqN;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DecidedMsg extends ProtoMessage {

    public static final short MSG_CODE = 203;

    public final int iN;
    public final SeqN sN;
    public final List<AcceptedValue> decidedValues;

    public DecidedMsg(int iN, SeqN sN, List<AcceptedValue> decidedValues) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.decidedValues = decidedValues;
    }

    @Override
    public String toString() {
        return "DecidedMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", decidedValues=" + decidedValues +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<DecidedMsg>() {
        public void serialize(DecidedMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeInt(msg.decidedValues.size());
            for (AcceptedValue v : msg.decidedValues)
                v.serialize(out);
        }

        public DecidedMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN seqN = SeqN.deserialize(in);
            int decidedValuesLength = in.readInt();
            List<AcceptedValue> decidedValues = new ArrayList<>(decidedValuesLength);
            for (int i = 0; i < decidedValuesLength; i++)
                decidedValues.add(AcceptedValue.deserialize(in));
            return new DecidedMsg(instanceNumber, seqN, decidedValues);
        }
    };
}
