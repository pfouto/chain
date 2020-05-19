package frontend.ops;

import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WriteBatch {

    private final long batchId;
    private final InetAddress issuer;
    private final List<byte[]> ops;

    public WriteBatch(long batchId, InetAddress issuer, List<byte[]> ops) {
        this.batchId = batchId;
        this.issuer = issuer;
        this.ops = ops;
    }

    public List<byte[]> getOps() {
        return ops;
    }

    public InetAddress getIssuer() {
        return issuer;
    }

    public long getBatchId() {
        return batchId;
    }

    @Override
    public String toString() {
        return "OpsBatch{" +
                "id=" + batchId +
                ", issuer=" + issuer +
                ", opsN=" + ops.size() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteBatch)) return false;
        WriteBatch writeBatch = (WriteBatch) o;
        return batchId == writeBatch.batchId &&
                issuer.equals(writeBatch.issuer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchId, issuer);
    }

    public static ISerializer<WriteBatch> serializer = new ISerializer<>() {
        @Override
        public void serialize(WriteBatch writeBatch, ByteBuf out) {
            out.writeLong(writeBatch.batchId);
            out.writeBytes(writeBatch.issuer.getAddress());
            out.writeInt(writeBatch.ops.size());
            for (byte[] op : writeBatch.ops) {
                out.writeInt(op.length);
                out.writeBytes(op);
            }
        }

        @Override
        public WriteBatch deserialize(ByteBuf in) throws UnknownHostException {
            long id = in.readLong();
            byte[] addrBytes = new byte[4];
            in.readBytes(addrBytes);
            int nOps = in.readInt();
            List<byte[]> ops = new ArrayList<>(nOps);
            for (int i = 0; i < nOps; i++) {
                int opDataSize = in.readInt();
                byte[] opData = new byte[opDataSize];
                in.readBytes(opData);
                ops.add(opData);
            }
            return new WriteBatch(id, InetAddress.getByAddress(addrBytes), ops);
        }
    };
}
