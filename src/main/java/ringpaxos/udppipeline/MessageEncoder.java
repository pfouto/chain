package ringpaxos.udppipeline;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;
import network.ISerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ringpaxos.utils.Multicast;

import java.util.List;

public class MessageEncoder extends MessageToMessageEncoder<ProtoMessage> {
    private static final Logger logger = LogManager.getLogger(MessageEncoder.class);

    private final ISerializer<ProtoMessage> serializer;

    public MessageEncoder(ISerializer<ProtoMessage> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ProtoMessage msg, List<Object> out) throws Exception {
        ByteBuf buffer = Unpooled.buffer();
        serializer.serialize(msg, buffer);
        if(buffer.writerIndex() > 65000)
            logger.error("Packet too big! " + buffer.writerIndex() + " " + msg);
        //logger.warn("Encoded size: " + buffer.writerIndex() + " " + (iSerializer.serializedSize(msg.payload) + 1));
        out.add(new DatagramPacket(buffer, Multicast.ADDR));

    }
}
