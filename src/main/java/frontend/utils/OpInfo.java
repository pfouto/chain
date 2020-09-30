package frontend.utils;

import network.data.Host;

public class OpInfo {
    private final Host client;
    private final int opId;
    private final byte opType;

    private OpInfo(Host client, int opId, byte opType) {
        this.client = client;
        this.opId = opId;
        this.opType = opType;
    }

    public static OpInfo of(Host client, int opId, byte opType) {
        return new OpInfo(client, opId, opType);
    }

    public byte getOpType() {
        return opType;
    }

    public Host getClient() {
        return client;
    }

    public int getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "OpInfo{" +
                "client=" + client +
                ", opId=" + opId +
                ", opType=" + opType +
                '}';
    }
}
