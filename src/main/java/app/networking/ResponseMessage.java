package app.networking;

public class ResponseMessage {

    private final int cId;
    private final byte[] response;

    public ResponseMessage(int cId, byte[] response){
        this.cId = cId;
        this.response = response;
    }

    public byte[] getResponse() {
        return response;
    }

    public int getcId() {
        return cId;
    }
}
