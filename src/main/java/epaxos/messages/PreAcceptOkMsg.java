package epaxos.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import common.values.PaxosValue;
import epaxos.utils.Ballot;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PreAcceptOkMsg extends ProtoMessage {

    public static final short MSG_CODE = 505;

    public final Ballot ballot;
    public final PaxosValue value;
    public final int seqNumber;
    public final Map<Host, Integer> deps;
    public final Host replica;
    public final int instanceNumber;

    public PreAcceptOkMsg(Ballot ballot, PaxosValue value, int seqNumber, Map<Host, Integer> deps, Host replica, int instanceNumber) {
        super(MSG_CODE);
        this.ballot = ballot;
        this.value = value;
        this.seqNumber = seqNumber;
        this.deps = deps;
        this.replica = replica;
        this.instanceNumber = instanceNumber;
    }

    @Override
    public String toString() {
        return "PreAcceptOkMsg{" +
                "b=" + ballot +
                "val=" + value +
                ", sN=" + seqNumber +
                ", deps=" + deps +
                ", rep=" + replica +
                ", iN=" + instanceNumber +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<PreAcceptOkMsg>() {
        public void serialize(PreAcceptOkMsg msg, ByteBuf out) throws IOException {
            msg.ballot.serialize(out);
            PaxosValue.serializer.serialize(msg.value, out);
            out.writeInt(msg.seqNumber);
            out.writeInt(msg.deps.size());
            for (Map.Entry<Host, Integer> entry : msg.deps.entrySet()) {
                Host k = entry.getKey();
                Integer v = entry.getValue();
                Host.serializer.serialize(k, out);
                out.writeInt(v);
            }
            Host.serializer.serialize(msg.replica, out);
            out.writeInt(msg.instanceNumber);
        }

        public PreAcceptOkMsg deserialize(ByteBuf in) throws IOException {
            Ballot b = Ballot.deserialize(in);
            PaxosValue value = PaxosValue.serializer.deserialize(in);
            int seqNumber = in.readInt();
            int depsSize = in.readInt();
            Map<Host, Integer> deps = new HashMap<>();
            for (int i = 0; i < depsSize; i++)
                deps.put(Host.serializer.deserialize(in), in.readInt());
            Host replica = Host.serializer.deserialize(in);
            int instanceNumber = in.readInt();
            return new PreAcceptOkMsg(b, value, seqNumber, deps, replica, instanceNumber);
        }

    };
}
