package ringpaxos.utils;

import babel.generic.ProtoMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import network.listeners.MessageListener;
import network.ISerializer;
import network.messaging.NetworkMessage;
import ringpaxos.udppipeline.MessageDecoder;
import ringpaxos.udppipeline.MessageEncoder;
import ringpaxos.udppipeline.MulticastConnectionHandler;
import ringpaxos.udppipeline.MulticastExceptionHandler;

import java.net.*;
import java.util.Map;

public class Multicast {

    NioDatagramChannel ch;

    public static InetSocketAddress ADDR;

    static {
        try {
            ADDR = new InetSocketAddress(InetAddress.getByName("239.10.10.10"), 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public Multicast(ISerializer<ProtoMessage> serializer, MessageListener<ProtoMessage> listener, int MAX_PACKET_SIZE)
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
        ch.writeAndFlush(msg);
    }
}
