package epaxos.utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Objects;

public class Ballot {

    public final int epoch;
    public final int b;
    public final Host replica;

    public Ballot(int epoch, int b, Host replica) {
        this.epoch = epoch;
        this.b = b;
        this.replica = replica;
    }

    @Override
    public String toString() {
        return "{" +
                "e=" + epoch +
                ", b=" + b +
                ", r=" + replica +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Ballot)) return false;
        Ballot ballot = (Ballot) o;
        return epoch == ballot.epoch &&
                b == ballot.b &&
                Objects.equals(replica, ballot.replica);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, b, replica);
    }

    public void serialize(ByteBuf out) throws IOException {
        out.writeInt(epoch);
        out.writeInt(b);
        Host.serializer.serialize(replica, out);
    }

    public static Ballot deserialize(ByteBuf in) throws IOException {
        int epoch = in.readInt();
        int b = in.readInt();
        Host replica = Host.serializer.deserialize(in);
        return new Ballot(epoch, b, replica);
    }

}
