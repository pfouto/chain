package distinguishedpaxos.utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Objects;

public class SeqN implements Comparable<SeqN> {
    private final int counter;
    private final Host node;

    public SeqN(int counter, Host node) {
        this.counter = counter;
        this.node = node;
    }

    public int getCounter() {
        return counter;
    }

    public Host getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "SN{" + counter + ":" + node + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SeqN)) return false;
        SeqN seqN = (SeqN) o;
        return counter == seqN.counter &&
                Objects.equals(node, seqN.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(counter, node);
    }

    @Override
    public int compareTo(SeqN o) {
        int compare = Integer.compare(this.counter, o.counter);
        return compare != 0 ? compare : this.node.compareTo(o.node);
    }

    public boolean greaterThan(SeqN other) {
        return this.compareTo(other) > 0;
    }

    public boolean greaterOrEqualsThan(SeqN other) {
        return this.compareTo(other) >= 0;
    }

    public boolean lesserThan(SeqN other) {
        return this.compareTo(other) < 0;
    }

    public boolean lesserOrEqualsThan(SeqN other) {
        return this.compareTo(other) <= 0;
    }

    public void serialize(ByteBuf out) throws IOException {
        out.writeInt(counter);
        Host.serializer.serialize(node, out);
    }

    public static SeqN deserialize(ByteBuf in) throws IOException {
        int sN = in.readInt();
        Host node = Host.serializer.deserialize(in);
        return new SeqN(sN, node);
    }
}
