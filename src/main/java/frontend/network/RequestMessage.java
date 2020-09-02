package frontend.network;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class RequestMessage extends ProtoMessage {

    public static final short MSG_CODE = 101;

    public static final byte READ_WEAK = 0;
    public static final byte READ_STRONG = 1;
    public static final byte WRITE = 2;

    private final int opId;
    private final byte opType;
    private final byte[] payload;

    public RequestMessage(int opId, byte opType, byte[] payload) {
        super(MSG_CODE);
        this.opId = opId;
        this.opType = opType;
        this.payload = payload;
    }

    public int getOpId() {
        return opId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public byte getOpType() {
        return opType;
    }

    @Override
    public String toString() {
        return "RequestMsg{" +
                "opId=" + opId +
                ", opType=" + opType +
                ", payload=" + payload.length +
                '}';
    }

    public static final ISerializer<RequestMessage> serializer = new ISerializer<>() {

        @Override
        public void serialize(RequestMessage requestMessage, ByteBuf out) {
            out.writeInt(requestMessage.opId);
            out.writeByte(requestMessage.opType);
            out.writeInt(requestMessage.payload.length);
            out.writeBytes(requestMessage.payload);
        }

        @Override
        public RequestMessage deserialize(ByteBuf in) {
            int opId = in.readInt();
            byte opType = in.readByte();
            int payloadSize = in.readInt();
            byte[] payload = new byte[payloadSize];
            in.readBytes(payload);
            return new RequestMessage(opId, opType, payload);
        }
    };
}
