package frontend.ops;

import io.netty.buffer.ByteBuf;

import java.util.Objects;

public class ReadOp {
    private final long opId;
    private final byte[] opData;

    public ReadOp(long opId, byte[] opData) {
        this.opId = opId;
        this.opData = opData;
    }

    public byte[] getOpData() {
        return opData;
    }

    public long getOpId() {
        return opId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReadOp readOp = (ReadOp) o;
        return opId == readOp.opId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(opId);
    }

    @Override
    public String toString() {
        return "AppOp{" +
                "opId=" + opId +
                '}';
    }

    public void serialize(ByteBuf out) {
        out.writeLong(opId);
        out.writeInt(opData.length);
        out.writeBytes(opData);
    }

    public static ReadOp deserialize(ByteBuf in) {
        long opId = in.readLong();
        int opDataSize = in.readInt();
        byte[] opData = new byte[opDataSize];
        in.readBytes(opData);
        return new ReadOp(opId, opData);
    }
}
