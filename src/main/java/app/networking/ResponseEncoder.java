package app.networking;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ResponseEncoder extends MessageToByteEncoder<ResponseMessage> {


    @Override
    protected void encode(ChannelHandlerContext ctx, ResponseMessage msg, ByteBuf out) throws Exception {
        out.writeInt(msg.getcId());
        out.writeInt(msg.getResponse().length);
        if(msg.getResponse().length > 0){
            out.writeBytes(msg.getResponse());
        }
    }
}