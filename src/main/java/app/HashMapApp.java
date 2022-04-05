package app;

import app.networking.RequestDecoder;
import app.networking.RequestMessage;
import app.networking.ResponseEncoder;
import app.networking.ResponseMessage;
import chainpaxos.ChainPaxosDelayedFront;
import chainpaxos.ChainPaxosDelayedProto;
import chainpaxos.ChainPaxosMixedFront;
import chainpaxos.ChainPaxosMixedProto;
import chainreplication.ChainRepMixedFront;
import chainreplication.ChainRepMixedProto;
import distinguishedpaxos.DistPaxosFront;
import distinguishedpaxos.DistPaxosPiggyProto;
import distinguishedpaxos.DistPaxosProto;
import distinguishedpaxos.MultiPaxosProto;
import epaxos.EPaxosFront;
import epaxos.EPaxosProto;
import epaxos.EsolatedPaxosProto;
import frontend.FrontendProto;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.InvalidParameterException;
import pt.unl.fct.di.novasys.babel.exceptions.ProtocolAlreadyExistsException;
import pt.unl.fct.di.novasys.network.NetworkManager;
import ringpaxos.RingPaxosFront;
import ringpaxos.RingPaxosPiggyProto;
import ringpaxos.RingPaxosProto;
import uringpaxos.URingPaxosFront;
import uringpaxos.URingPaxosProto;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HashMapApp implements Application {

    private static final Logger logger = LogManager.getLogger(HashMapApp.class);
    private final ConcurrentMap<Integer, Pair<Integer, Channel>> opMapper;
    private final AtomicInteger idCounter;
    private final List<FrontendProto> frontendProtos;
    private int nWrites,nReads;
    private ConcurrentMap<String, byte[]> store;

    public HashMapApp(Properties configProps) throws IOException, ProtocolAlreadyExistsException,
            HandlerRegistrationException, InterruptedException {

        store = new ConcurrentHashMap<>();
        idCounter = new AtomicInteger(0);
        opMapper = new ConcurrentHashMap<>();
        int port = Integer.parseInt(configProps.getProperty("app_port"));
        Babel babel = Babel.getInstance();
        EventLoopGroup consensusWorkerGroup = NetworkManager.createNewWorkerGroup();
        String alg = configProps.getProperty("algorithm");
        int nFrontends = Short.parseShort(configProps.getProperty("n_frontends"));
        frontendProtos = new LinkedList<>();
        GenericProtocol consensusProto;

        switch (alg) {
            case "chain_mixed":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosMixedFront(configProps, i, this));
                consensusProto = new ChainPaxosMixedProto(configProps, consensusWorkerGroup);
                break;
            case "chain_mixed_2":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosMixedFront(configProps, i, this));
                consensusProto = new ChainPaxosMixedProto(configProps, consensusWorkerGroup);
                break;
            case "chain_mixed_3":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosMixedFront(configProps, i, this));
                consensusProto = new ChainPaxosMixedProto(configProps, consensusWorkerGroup);
                break;
            case "chain_delayed":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainPaxosDelayedFront(configProps, i, this));
                consensusProto = new ChainPaxosDelayedProto(configProps, consensusWorkerGroup);
                break;
            case "chainrep":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new ChainRepMixedFront(configProps, i, this));
                consensusProto = new ChainRepMixedProto(configProps, consensusWorkerGroup);
                break;
            case "distinguished":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new DistPaxosFront(configProps, i, this));
                consensusProto = new DistPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "distinguished_piggy":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new DistPaxosFront(configProps, i, this));
                consensusProto = new DistPaxosPiggyProto(configProps, consensusWorkerGroup);
                break;
            case "multi":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new DistPaxosFront(configProps, i, this));
                consensusProto = new MultiPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "epaxos":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new EPaxosFront(configProps, i, this));
                consensusProto = new EPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "esolatedpaxos":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new EPaxosFront(configProps, i, this));
                consensusProto = new EsolatedPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "ring":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new RingPaxosFront(configProps, i, this));
                consensusProto = new RingPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "uring":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new URingPaxosFront(configProps, i, this));
                consensusProto = new URingPaxosProto(configProps, consensusWorkerGroup);
                break;
            case "ringpiggy":
                for (short i = 0; i < nFrontends; i++)
                    frontendProtos.add(new RingPaxosFront(configProps, i, this));
                consensusProto = new RingPaxosPiggyProto(configProps, consensusWorkerGroup);
                break;
            default:
                logger.error("Unknown algorithm: " + alg);
                System.exit(-1);
                return;
        }

        for (FrontendProto frontendProto : frontendProtos)
            babel.registerProtocol(frontendProto);
        babel.registerProtocol(consensusProto);

        for (FrontendProto frontendProto : frontendProtos)
            frontendProto.init(configProps);
        consensusProto.init(configProps);

        babel.start();

        Runtime.getRuntime().addShutdownHook( new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("Writes: " + nWrites + ", reads: " + nReads);
            }
        }));

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RequestDecoder(), new ResponseEncoder(), new ServerHandler());
                        }
                    }).option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            logger.debug("Listening: " + f.channel().localAddress());
            f.channel().closeFuture().sync();
            logger.info("Server channel closed");
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }

    public static void main(String[] args) throws InvalidParameterException, IOException,
            HandlerRegistrationException, ProtocolAlreadyExistsException, InterruptedException {
        Properties configProps =
                Babel.loadConfig(Arrays.copyOfRange(args, 0, args.length), "config.properties");
        logger.debug(configProps);
        if (configProps.containsKey("interface")) {
            String address = getAddress(configProps.getProperty("interface"));
            if (address == null) return;
            configProps.setProperty(FrontendProto.ADDRESS_KEY, address);
            configProps.setProperty(ChainPaxosMixedProto.ADDRESS_KEY, address);
        }
        new HashMapApp(configProps);
    }

    private static String getAddress(String inter) throws SocketException {
        NetworkInterface byName = NetworkInterface.getByName(inter);
        if (byName == null) {
            logger.error("No interface named " + inter);
            return null;
        }
        Enumeration<InetAddress> addresses = byName.getInetAddresses();
        InetAddress currentAddress;
        while (addresses.hasMoreElements()) {
            currentAddress = addresses.nextElement();
            if (currentAddress instanceof Inet4Address)
                return currentAddress.getHostAddress();
        }
        logger.error("No ipv4 found for interface " + inter);
        return null;
    }

    //Called by **single** frontend thread
    @Override
    public void executeOperation(byte[] opData, boolean local, long instId) {
        HashMapOp op;
        try {
            op = HashMapOp.fromByteArray(opData);
        } catch (IOException e) {
            logger.error("Error decoding opData", e);
            throw new AssertionError("Error decoding opData");
        }
        //logger.info("Exec op: " + op + (local ? "local" : ""));

        Pair<Integer, Channel> opInfo = local ? opMapper.remove(op.getId()) : null;
        if (op.getRequestType() == RequestMessage.WRITE) {
            store.put(op.getRequestKey(), op.getRequestValue());
            nWrites++;
            if (local) {
                opInfo.getRight().writeAndFlush(new ResponseMessage(opInfo.getLeft(), new byte[0]));
                if(logger.isDebugEnabled()) logger.debug("Responding");

            }
        } else { //READ
            if (local) {
                nReads++;
                opInfo.getRight().writeAndFlush(
                        new ResponseMessage(opInfo.getLeft(), store.getOrDefault(op.getRequestKey(), new byte[0])));
                if(logger.isDebugEnabled()) logger.debug("Responding");
            } //If remote read, nothing to do
        }
    }

    @Override
    public void installState(byte[] state) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(state);
            DataInputStream in = new DataInputStream(bais);
            nWrites = in.readInt();
            int mapSize = in.readInt();
            store = new ConcurrentHashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String key = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.read(value);
                store.put(key, value);
            }
            logger.info("State installed(" + nWrites + ") ");
        } catch (IOException e) {
            logger.error("Error installing state", e);
            throw new AssertionError();
        }
    }

    @Override
    public byte[] getSnapshot() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(nWrites);
            out.writeInt(store.size());
            for (Map.Entry<String, byte[]> e : store.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeInt(e.getValue().length);
                out.write(e.getValue());
            }
            logger.info("State stored(" + nWrites + "), size: " + baos.size() );
            return baos.toByteArray();
        } catch (IOException e) {
            logger.error("Error generating state", e);
            throw new AssertionError();
        }
    }

    class ServerHandler extends ChannelInboundHandlerAdapter {
        //Called by netty threads
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.debug("Client connected: " + ctx.channel().remoteAddress());
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.debug("Client connection lost: " + ctx.channel().remoteAddress());
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception caught.", cause);
            //ctx.fireExceptionCaught(cause);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            logger.info("Unexpected event: " + evt);
            ctx.fireUserEventTriggered(evt);
        }

        //Called by netty threads
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            RequestMessage rMsg = (RequestMessage) msg;
            if(logger.isDebugEnabled()) logger.debug("Client op: " + msg);
            if (rMsg.getRequestType() == RequestMessage.WEAK_READ) { //Exec immediately and respond
                byte[] bytes = store.get(rMsg.getRequestKey());
                ctx.channel().writeAndFlush(new ResponseMessage(rMsg.getcId(), bytes == null ? new byte[0] : bytes));
            } else { //Submit to consensus
                int id = idCounter.incrementAndGet();
                opMapper.put(id, Pair.of(rMsg.getcId(), ctx.channel()));
                byte[] opData = HashMapOp.toByteArray(id, rMsg.getRequestType(), rMsg.getRequestKey(), rMsg.getRequestValue());
                frontendProtos.get(0).submitOperation(opData, rMsg.getRequestType() == RequestMessage.WRITE ?
                        FrontendProto.OpType.WRITE : FrontendProto.OpType.STRONG_READ);
            }
        }

    }

}
