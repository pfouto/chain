package uringpaxos.messages;

import common.values.PaxosValue;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import uringpaxos.utils.SeqN;

import java.io.IOException;

public class AcceptMsg extends ProtoMessage {

    public static final short MSG_CODE = 602;

    public final int iN;
    public final SeqN sN;
    public final PaxosValue value;
    public final long valueId;
    public final int highestDecided;

    public AcceptMsg(int iN, SeqN sN, PaxosValue value, long valueId, int highestDecided) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.value = value;
        this.valueId = valueId;
        this.highestDecided = highestDecided;
    }

    @Override
    public String toString() {
        return "AcceptMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", value=" + value +
                ", valueId=" + valueId +
                ", highestDecided=" + highestDecided +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptMsg>() {
        public void serialize(AcceptMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeBoolean(msg.value != null);
            if(msg.value != null)
                PaxosValue.serializer.serialize(msg.value, out);
            out.writeLong(msg.valueId);
            out.writeInt(msg.highestDecided);

        }

        public AcceptMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            boolean hasValue = in.readBoolean();
            PaxosValue payload = null;
            if(hasValue){
                payload = PaxosValue.serializer.deserialize(in);
            }
            long valueId = in.readLong();
            int highestDecided = in.readInt();
            return new AcceptMsg(instanceNumber, sN, payload, valueId, highestDecided);
        }
    };
}
