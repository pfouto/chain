package frontend.network;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ResponseMessage extends ProtoMessage {

    public static final short MSG_CODE = 102;

    private final int opId;
    private final byte opType;
    private final byte[] response;

    public ResponseMessage(int opId, byte opType, byte[] response) {
        super(MSG_CODE);
        this.opId = opId;
        this.opType = opType;
        this.response = response;
    }

    public int getOpId() {
        return opId;
    }

    public byte[] getResponse() {
        return response;
    }

    public byte getOpType() {
        return opType;
    }

    @Override
    public String toString() {
        return "ResponseMsg{" +
                "opId=" + opId +
                ", opType=" + opType +
                ", response=" + response.length +
                '}';
    }

    public static final ISerializer<ResponseMessage> serializer = new ISerializer<>() {

        @Override
        public void serialize(ResponseMessage requestMessage, ByteBuf out) {
            out.writeInt(requestMessage.opId);
            out.writeByte(requestMessage.opType);
            out.writeInt(requestMessage.response.length);
            out.writeBytes(requestMessage.response);
        }

        @Override
        public ResponseMessage deserialize(ByteBuf in) {
            int opId = in.readInt();
            byte opType = in.readByte();
            int payloadSize = in.readInt();
            byte[] payload = new byte[payloadSize];
            in.readBytes(payload);
            return new ResponseMessage(opId, opType, payload);
        }
    };
}
