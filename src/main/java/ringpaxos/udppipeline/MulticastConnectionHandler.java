package ringpaxos.udppipeline;

import babel.generic.ProtoMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import network.listeners.MessageListener;
import network.messaging.NetworkMessage;
import network.messaging.control.ControlMessage;

import java.util.Map;

public class MulticastConnectionHandler extends SimpleChannelInboundHandler<ProtoMessage> {

    MessageListener<ProtoMessage> listener;

    public MulticastConnectionHandler(MessageListener<ProtoMessage> listener){
        this.listener = listener;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ProtoMessage msg) {
        listener.deliverMessage(msg, null);
    }
}
