package ringpaxos.udppipeline;

import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class MessageDecoder extends MessageToMessageDecoder<DatagramPacket> {

    private static final Logger logger = LogManager.getLogger(MessageDecoder.class);

    private final ISerializer<BabelMessage> serializer;

    public MessageDecoder(ISerializer<BabelMessage> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> out) throws Exception {
        try{
            ByteBuf in = packet.content();
            BabelMessage deserialize = serializer.deserialize(in);
            out.add(deserialize);
        } catch (Exception e){
            logger.error("Decode error: " + e.getMessage());
        }
    }
}
