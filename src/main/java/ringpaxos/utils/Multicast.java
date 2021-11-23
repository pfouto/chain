package ringpaxos.utils;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import pt.unl.fct.di.novasys.babel.internal.BabelMessage;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;
import pt.unl.fct.di.novasys.network.ISerializer;
import ringpaxos.udppipeline.MessageDecoder;
import ringpaxos.udppipeline.MessageEncoder;
import ringpaxos.udppipeline.MulticastConnectionHandler;
import ringpaxos.udppipeline.MulticastExceptionHandler;

import java.net.*;
import java.util.Map;

public class Multicast {

    public static InetSocketAddress ADDR;

    static {
        try {
            ADDR = new InetSocketAddress(InetAddress.getByName("239.10.10.10"), 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    NioDatagramChannel ch;

    public Multicast(ISerializer<BabelMessage> serializer, MessageListener<BabelMessage> listener, int MAX_PACKET_SIZE)
            throws InterruptedException, SocketException {

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(bossGroup)
                .channelFactory(
                        (ChannelFactory<NioDatagramChannel>) () -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(NioDatagramChannel ch) {
                        ch.pipeline().addLast("MessageDecoder", new MessageDecoder(serializer));
                        ch.pipeline().addLast("MessageEncoder", new MessageEncoder(serializer));
                        ch.pipeline().addLast("MulticastConnectionHandler", new MulticastConnectionHandler(listener));
                        ch.pipeline().addLast("MulticastExceptionHandler", new MulticastExceptionHandler());
                    }
                })
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(MAX_PACKET_SIZE + 1000));

        ch = (NioDatagramChannel) b.bind(ADDR.getPort()).sync().channel();
        ch.joinGroup(ADDR, NetworkInterface.getByName("br0")).sync();
        ch.closeFuture().addListener(cf -> {
            bossGroup.shutdownGracefully();
        });
    }

    public void sendMulticast(ProtoMessage msg) {
        ch.writeAndFlush(new BabelMessage(msg, (short)-1, (short)-1));
    }
}
