package frontend.ops;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OpBatch {

    private final long batchId;
    private final InetAddress issuer;
    private final short frontendId;
    private final List<byte[]> ops;

    public OpBatch(long batchId, InetAddress issuer, short frontendId, List<byte[]> ops) {
        this.batchId = batchId;
        this.issuer = issuer;
        this.ops = ops;
        this.frontendId = frontendId;
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

    public short getFrontendId() {
        return frontendId;
    }


    @Override
    public String toString() {
        return "OpBatch{" +
                "batchId=" + batchId +
                ", issuer=" + issuer +
                ", frontendId=" + frontendId +
                ", ops=" + ops +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpBatch opBatch = (OpBatch) o;
        return batchId == opBatch.batchId &&
                frontendId == opBatch.frontendId &&
                Objects.equals(issuer, opBatch.issuer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchId, issuer, frontendId);
    }

    public static ISerializer<OpBatch> serializer = new ISerializer<>() {
        @Override
        public void serialize(OpBatch opBatch, ByteBuf out) {
            out.writeLong(opBatch.batchId);
            out.writeBytes(opBatch.issuer.getAddress());
            out.writeShort(opBatch.frontendId);
            out.writeInt(opBatch.ops.size());
            for (byte[] op : opBatch.ops) {
                out.writeInt(op.length);
                out.writeBytes(op);
            }
        }

        @Override
        public OpBatch deserialize(ByteBuf in) throws UnknownHostException {
            long id = in.readLong();
            byte[] addrBytes = new byte[4];
            in.readBytes(addrBytes);
            short frontendId = in.readShort();
            int nOps = in.readInt();
            List<byte[]> ops = new ArrayList<>(nOps);
            for (int i = 0; i < nOps; i++) {
                int opDataSize = in.readInt();
                byte[] opData = new byte[opDataSize];
                in.readBytes(opData);
                ops.add(opData);
            }
            return new OpBatch(id, InetAddress.getByAddress(addrBytes), frontendId, ops);
        }
    };
}
