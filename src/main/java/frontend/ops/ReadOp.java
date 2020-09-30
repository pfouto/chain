package frontend.ops;

import io.netty.buffer.ByteBuf;
import network.data.Host;

import java.util.Arrays;
import java.util.Objects;

public class ReadOp {
    private final int opId;
    private final byte[] opData;
    private final Host client;

    public ReadOp(int opId, byte[] opData, Host client) {
        this.opId = opId;
        this.opData = opData;
        this.client = client;
    }

    public Host getClient() {
        return client;
    }

    public byte[] getOpData() {
        return opData;
    }

    public int getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "ReadOp{" +
                "opId=" + opId +
                ", opData=" + Arrays.toString(opData) +
                ", client=" + client +
                '}';
    }
}
