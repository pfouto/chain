package app.networking;

import java.util.Arrays;

public class RequestMessage {

    public final static byte WRITE = 0;
    public final static byte WEAK_READ = 1;
    public final static byte STRONG_READ = 2;

    private final int cId;
    private final byte requestType;
    private final String requestKey;
    private final byte[] requestValue;

    public RequestMessage(int cId, byte requestType, String requestKey, byte[] requestValue){
        this.cId = cId;
        this.requestType = requestType;
        this.requestKey = requestKey;
        this.requestValue = requestValue;
    }

    public byte[] getRequestValue() {
        return requestValue;
    }

    public byte getRequestType() {
        return requestType;
    }

    public String getRequestKey() {
        return requestKey;
    }

    public int getcId() {
        return cId;
    }

    @Override
    public String toString() {
        return "RequestMessage{" +
                "cId=" + cId +
                ", requestType=" + requestType +
                ", requestKey='" + requestKey + '\'' +
                ", requestValueSize=" + requestValue.length +
                '}';
    }
}
