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

public class PreAcceptMsg extends ProtoMessage {

    public static final short MSG_CODE = 504;

    public final Ballot ballot;
    public final PaxosValue value;
    public final int sN;
    public final Map<Host, Integer> deps;
    public final Host replica;
    public final int iN;

    public PreAcceptMsg(Ballot ballot, PaxosValue value, int sN, Map<Host, Integer> deps, Host replica, int iN) {
        super(MSG_CODE);
        this.ballot = ballot;
        this.value = value;
        this.sN = sN;
        this.deps = deps;
        this.replica = replica;
        this.iN = iN;
    }

    public String toString() {
        return "PreAcceptMsg{" +
                "bal=" + ballot +
                "val=" + value +
                ", sN=" + sN +
                ", deps=" + deps +
                ", rep=" + replica +
                ", iN=" + iN +
                '}';
    }

    public static ISerializer<? extends ProtoMessage> serializer = new ISerializer<PreAcceptMsg>() {
        public void serialize(PreAcceptMsg msg, ByteBuf out) throws IOException {
            msg.ballot.serialize(out);
            PaxosValue.serializer.serialize(msg.value, out);
            out.writeInt(msg.sN);
            out.writeInt(msg.deps.size());
            for (Map.Entry<Host, Integer> entry : msg.deps.entrySet()) {
                Host k = entry.getKey();
                Integer v = entry.getValue();
                Host.serializer.serialize(k, out);
                out.writeInt(v);
            }
            Host.serializer.serialize(msg.replica, out);
            out.writeInt(msg.iN);
        }

        public PreAcceptMsg deserialize(ByteBuf in) throws IOException {
            Ballot b = Ballot.deserialize(in);
            PaxosValue value = PaxosValue.serializer.deserialize(in);
            int seqNumber = in.readInt();
            int depsSize = in.readInt();
            Map<Host, Integer> deps = new HashMap<>();
            for (int i = 0; i < depsSize; i++)
                deps.put(Host.serializer.deserialize(in), in.readInt());
            Host replica = Host.serializer.deserialize(in);
            int instanceNumber = in.readInt();
            return new PreAcceptMsg(b, value, seqNumber, deps, replica, instanceNumber);
        }
    };
}
