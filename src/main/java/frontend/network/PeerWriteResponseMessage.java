package frontend.network;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class PeerWriteResponseMessage extends ProtoMessage {

    public static final short MSG_CODE = 106;

    private final long batchId;

    public PeerWriteResponseMessage(long batchId) {
        super(MSG_CODE);
        this.batchId = batchId;
    }

    public long getBatchId() {
        return batchId;
    }

    @Override
    public String toString() {
        return "RedirectMessage{" +
                "batchId=" + batchId +
                '}';
    }

    public static final ISerializer<PeerWriteResponseMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PeerWriteResponseMessage peerWriteMessage, ByteBuf out) throws IOException {
            out.writeLong(peerWriteMessage.batchId);
        }

        @Override
        public PeerWriteResponseMessage deserialize(ByteBuf in) throws IOException {
            long batchId = in.readLong();
            return new PeerWriteResponseMessage(batchId);
        }
    };
}
