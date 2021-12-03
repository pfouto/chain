package app.networking;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.concurrent.EventExecutorGroup;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RequestDecoder extends ReplayingDecoder<RequestDecoder.RequestDecodedState> {

    private int cId;
    private byte requestType;
    private String requestKey;

    public RequestDecoder() {
        super(RequestDecodedState.READ_CID);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case READ_CID:
                cId = in.readInt();
                checkpoint(RequestDecodedState.READ_TYPE);
            case READ_TYPE:
                requestType = in.readByte();
                checkpoint(RequestDecodedState.READ_KEY);
            case READ_KEY:
                int keyLen = in.readInt();
                requestKey = in.readCharSequence(keyLen, StandardCharsets.UTF_8).toString();
                checkpoint(RequestDecodedState.READ_VALUE);
            case READ_VALUE:
                byte[] requestValue;
                if (requestType == RequestMessage.WRITE) {
                    int valLen = in.readInt();
                    requestValue = new byte[valLen];
                    in.readBytes(requestValue);
                } else {
                    requestValue = new byte[0];
                }
                checkpoint(RequestDecodedState.READ_TYPE);
                out.add(new RequestMessage(cId, requestType, requestKey, requestValue));
                break;
            default:
                throw new Error("Shouldn't reach here.");
        }
    }

    public enum RequestDecodedState {
        READ_CID,
        READ_TYPE,
        READ_KEY,
        READ_VALUE;
    }
}
