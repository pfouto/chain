package ringpaxos.udppipeline;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;
import pt.unl.fct.di.novasys.network.messaging.NetworkMessage;
import pt.unl.fct.di.novasys.network.messaging.control.ControlMessage;

import java.util.Map;

public class MulticastConnectionHandler extends SimpleChannelInboundHandler<BabelMessage> {

    MessageListener<BabelMessage> listener;

    public MulticastConnectionHandler(MessageListener<BabelMessage> listener){
        this.listener = listener;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, BabelMessage msg) {
        listener.deliverMessage(msg, null);
    }
}
