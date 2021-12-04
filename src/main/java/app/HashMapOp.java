package app;

import app.networking.RequestMessage;

import java.io.*;
import java.util.Arrays;

public class HashMapOp {

    private final int id;
    private final byte requestType;
    private final String requestKey;
    private final byte[] requestValue;

    public HashMapOp(int id, byte type, String requestKey, byte[] requestValue) {
        this.id = id;
        this.requestType = type;
        this.requestValue = requestValue;
        this.requestKey = requestKey;
    }

    public static HashMapOp fromByteArray(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream in = new DataInputStream(bais);
        int id = in.readInt();
        byte requestType = in.readByte();
        String requestKey = in.readUTF();
        byte[] requestValue;
        if (requestType == RequestMessage.WRITE) {
            requestValue = new byte[in.readInt()];
            in.read(requestValue);
        } else {
            requestValue = new byte[0];
        }
        return new HashMapOp(id, requestType, requestKey, requestValue);
    }

    public String getRequestKey() {
        return requestKey;
    }

    public byte[] getRequestValue() {
        return requestValue;
    }

    public byte getRequestType() {
        return requestType;
    }

    public int getId() {
        return id;
    }

    public byte[] toByteArray() throws IOException {
        return toByteArray(id, requestType, requestKey, requestValue);
    }

    public static byte[] toByteArray(int id, byte type, String requestKey, byte[] requestValue) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(id);
        out.writeByte(type);
        out.writeUTF(requestKey);
        if (type == RequestMessage.WRITE) {
            out.writeInt(requestValue.length);
            out.write(requestValue);
        }
        return baos.toByteArray();
    }

    @Override
    public String toString() {
        return "HashMapOp{" +
                "id=" + id +
                ", requestType=" + requestType +
                ", requestKey='" + requestKey + '\'' +
                ", requestValueSize=" + requestValue.length +
                '}';
    }
}
