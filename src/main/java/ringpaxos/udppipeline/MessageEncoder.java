package ringpaxos.udppipeline;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ringpaxos.utils.Multicast;

import java.util.List;

public class MessageEncoder extends MessageToMessageEncoder<BabelMessage> {
    private static final Logger logger = LogManager.getLogger(MessageEncoder.class);

    private final ISerializer<BabelMessage> serializer;

    public MessageEncoder(ISerializer<BabelMessage> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, BabelMessage msg, List<Object> out) throws Exception {
        ByteBuf buffer = Unpooled.buffer();
        serializer.serialize(msg, buffer);
        if(buffer.writerIndex() > 65000)
            logger.error("Packet too big! " + buffer.writerIndex() + " " + msg);
        //logger.warn("Encoded size: " + buffer.writerIndex() + " " + (iSerializer.serializedSize(msg.payload) + 1));
        out.add(new DatagramPacket(buffer, Multicast.ADDR));

    }
}
