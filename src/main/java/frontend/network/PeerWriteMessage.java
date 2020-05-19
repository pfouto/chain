package frontend.network;

import babel.generic.ProtoMessage;
import common.WriteBatch;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.IOException;

public class PeerWriteMessage extends ProtoMessage {

    public static final short MSG_CODE = 103;

    private final WriteBatch batch;

    public PeerWriteMessage(WriteBatch batch) {
        super(MSG_CODE);
        this.batch = batch;
    }

    public WriteBatch getBatch() {
        return batch;
    }

    @Override
    public String toString() {
        return "RedirectMessage{" +
                "batch=" + batch +
                '}';
    }

    public static final ISerializer<PeerWriteMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PeerWriteMessage peerWriteMessage, ByteBuf out) throws IOException {
            WriteBatch.serializer.serialize(peerWriteMessage.batch, out);
        }

        @Override
        public PeerWriteMessage deserialize(ByteBuf in) throws IOException {
            WriteBatch batch = WriteBatch.serializer.deserialize(in);
            return new PeerWriteMessage(batch);
        }
    };
}
