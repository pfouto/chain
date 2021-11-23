package epaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import common.values.PaxosValue;
import epaxos.utils.Ballot;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class AcceptOkMsg extends ProtoMessage {

    public static final short MSG_CODE = 502;

    public final Ballot ballot;
    public final PaxosValue value;
    public final Host replica;
    public final int instanceNumber;

    public AcceptOkMsg(Ballot ballot, PaxosValue value, Host replica, int instanceNumber) {
        super(MSG_CODE);
        this.ballot = ballot;
        this.value = value;
        this.replica = replica;
        this.instanceNumber = instanceNumber;
    }

    @Override
    public String toString() {
        return "AcceptOkMsg{" +
                "ballot=" + ballot +
                "value=" + value +
                ", replica=" + replica +
                ", iN=" + instanceNumber +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<AcceptOkMsg>() {
        public void serialize(AcceptOkMsg msg, ByteBuf out) throws IOException {
            msg.ballot.serialize(out);
            PaxosValue.serializer.serialize(msg.value, out);
            Host.serializer.serialize(msg.replica, out);
            out.writeInt(msg.instanceNumber);
        }

        public AcceptOkMsg deserialize(ByteBuf in) throws IOException {
            Ballot b = Ballot.deserialize(in);
            PaxosValue value = PaxosValue.serializer.deserialize(in);
            Host replica = Host.serializer.deserialize(in);
            int instanceNumber = in.readInt();
            return new AcceptOkMsg(b, value, replica, instanceNumber);
        }
    };
}
