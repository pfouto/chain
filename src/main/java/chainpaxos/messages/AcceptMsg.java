package chainpaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import chainpaxos.utils.SeqN;
import common.values.PaxosValue;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class AcceptMsg extends ProtoMessage {

    public static final short MSG_CODE = 202;

    public final int iN;
    public final SeqN sN;
    public final PaxosValue value;
    public final short nodeCounter;
    public final int ack;

    public AcceptMsg(int iN, SeqN sN, short nodeCounter, PaxosValue value, int ack) {
        super(MSG_CODE);
        this.iN = iN;
        this.sN = sN;
        this.value = value;
        this.nodeCounter = nodeCounter;
        this.ack = ack;
    }

    @Override
    public String toString() {
        return "AcceptMsg{" +
                "iN=" + iN +
                ", sN=" + sN +
                ", value=" + value +
                ", nodeCounter=" + nodeCounter +
                ", ack=" + ack +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptMsg>() {
        public void serialize(AcceptMsg msg, ByteBuf out) throws IOException {
            out.writeInt(msg.iN);
            msg.sN.serialize(out);
            out.writeShort(msg.nodeCounter);
            PaxosValue.serializer.serialize(msg.value, out);
            out.writeInt(msg.ack);
        }

        public AcceptMsg deserialize(ByteBuf in) throws IOException {
            int instanceNumber = in.readInt();
            SeqN sN = SeqN.deserialize(in);
            short nodeCount = in.readShort();
            PaxosValue payload = PaxosValue.serializer.deserialize(in);
            int ack = in.readInt();
            return new AcceptMsg(instanceNumber, sN, nodeCount, payload, ack);
        }
    };
}
