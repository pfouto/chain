package frontend;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleServerChannel;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientDownEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ClientUpEvent;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import frontend.ipc.DeliverSnapshotReply;
import frontend.ipc.GetSnapshotRequest;
import frontend.network.*;
import frontend.notifications.*;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;

public abstract class FrontendProto extends GenericProtocol {

    public static final String ADDRESS_KEY = "frontend_address";
    public static final String PEER_PORT_KEY = "frontend_peer_port";
    public static final String SERVER_PORT_KEY = "frontend_server_port";
    private static final Logger logger = LogManager.getLogger(FrontendProto.class);
    protected final int PEER_PORT;
    protected final int SERVER_PORT;
    protected final InetAddress self;
    private final int opPrefix;
    private final EventLoopGroup workerGroup;
    private final short protoIndex;
    protected int serverChannel;
    protected int peerChannel;

    protected List<InetAddress> membership;
    private int opCounter;
    //App state
    private int nWrites;
    private byte[] incrementalHash;

    public FrontendProto(String protocolName, short protocolId, Properties props,
                         EventLoopGroup workerGroup, short protoIndex) throws IOException {
        super(protocolName, protocolId);

        this.PEER_PORT = Integer.parseInt(props.getProperty(PEER_PORT_KEY)) + protoIndex;
        this.SERVER_PORT = Integer.parseInt(props.getProperty(SERVER_PORT_KEY)) + protoIndex;

        this.workerGroup = workerGroup;
        self = InetAddress.getByName(props.getProperty(ADDRESS_KEY));
        opPrefix = ByteBuffer.wrap(self.getAddress()).getInt();
        opCounter = 0;
        membership = null;

        nWrites = 0;
        incrementalHash = new byte[0];
        this.protoIndex = protoIndex;
    }

    private static byte[] sha1(byte[] oldHash, long newData) {
        MessageDigest mDigest;
        try {
            mDigest = MessageDigest.getInstance("sha-256");
        } catch (NoSuchAlgorithmException e) {
            logger.error("MD5 not available...");
            throw new AssertionError("MD5 not available...");
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.write(oldHash);
            dos.writeLong(newData);
            return mDigest.digest(baos.toByteArray());
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new AssertionError();
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        //Server
        Properties serverProps = new Properties();
        serverProps.setProperty(SimpleServerChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        serverProps.setProperty(SimpleServerChannel.PORT_KEY, Integer.toString(SERVER_PORT));
        serverProps.put(SimpleServerChannel.WORKER_GROUP_KEY, workerGroup);
        //serverProps.put(SimpleServerChannel.DEBUG_INTERVAL_KEY, 10000);
        serverChannel = createChannel(SimpleServerChannel.NAME, serverProps);
        if (protoIndex == 0) {
            registerMessageSerializer(serverChannel, RequestMessage.MSG_CODE, RequestMessage.serializer);
            registerMessageSerializer(serverChannel, ResponseMessage.MSG_CODE, ResponseMessage.serializer);
        }
        registerMessageHandler(serverChannel, RequestMessage.MSG_CODE, this::onRequestMessage);
        registerMessageHandler(serverChannel, ResponseMessage.MSG_CODE, null, this::onResponseMessageFail);
        registerChannelEventHandler(serverChannel, ClientUpEvent.EVENT_ID, this::onClientUp);
        registerChannelEventHandler(serverChannel, ClientDownEvent.EVENT_ID, this::onClientDown);

        //Peer
        Properties peerProps = new Properties();
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, Integer.toString(PEER_PORT));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        //peerProps.put(TCPChannel.DEBUG_INTERVAL_KEY, 10000);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        if (protoIndex == 0) {
            registerMessageSerializer(peerChannel, PeerReadMessage.MSG_CODE, PeerReadMessage.serializer);
            registerMessageSerializer(peerChannel, PeerWriteMessage.MSG_CODE, PeerWriteMessage.serializer);
            registerMessageSerializer(peerChannel, PeerReadResponseMessage.MSG_CODE, PeerReadResponseMessage.serializer);
            registerMessageSerializer(peerChannel, PeerWriteResponseMessage.MSG_CODE, PeerWriteResponseMessage.serializer);
        }
        registerMessageHandler(peerChannel, PeerReadMessage.MSG_CODE, this::onPeerReadMessage,
                this::uponMessageFailed);
        registerMessageHandler(peerChannel, PeerReadResponseMessage.MSG_CODE, this::onPeerReadResponseMessage,
                this::uponMessageFailed);
        registerMessageHandler(peerChannel, PeerWriteMessage.MSG_CODE, this::onPeerWriteMessage,
                this::uponMessageFailed);
        registerMessageHandler(peerChannel, PeerWriteResponseMessage.MSG_CODE, this::onPeerWriteResponseMessage,
                this::uponMessageFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);

        //Consensus
        subscribeNotification(MembershipChange.NOTIFICATION_ID, this::onMembershipChange);
        subscribeNotification(ExecuteBatchNotification.NOTIFICATION_ID, this::onExecuteBatch);
        registerReplyHandler(ExecuteReadReply.REPLY_ID, this::onExecuteRead);
        subscribeNotification(InstallSnapshotNotification.NOTIFICATION_ID, this::onInstallSnapshot);
        registerRequestHandler(GetSnapshotRequest.REQUEST_ID, this::onGetStateSnapshot);
        _init(props);
    }

    protected abstract void _init(Properties props) throws HandlerRegistrationException;

    /* ---------------------------------------------- ---------- ---------------------------------------------- */
    /* ---------------------------------------------- CLIENT OPS ---------------------------------------------- */
    /* ---------------------------------------------- ---------- ---------------------------------------------- */

    protected long nextId() {
        //Message id is constructed using the server ip and a local counter (makes it unique and sequential)
        //TODO test if results in incrementing ids
        opCounter++;
        return ((long) opCounter << 32) | (opPrefix & 0xFFFFFFFFL);
    }

    private void onClientUp(ClientUpEvent event, int channel) {
        logger.debug(event);
    }

    private void onClientDown(ClientDownEvent event, int channel) {
        logger.debug(event);
    }

    protected abstract void onRequestMessage(RequestMessage msg, Host from, short sProto, int channel);

    /* ----------------------------------------------- -------- ----------------------------------------------- */
    /* ----------------------------------------------- PEER OPS ----------------------------------------------- */
    /* ----------------------------------------------- -------- ----------------------------------------------- */

    private void onResponseMessageFail(ResponseMessage message, Host host, short dProto, Throwable cause, int channel) {
        logger.warn(message + " failed to " + host + " - " + cause);
    }

    protected abstract void onPeerReadMessage(PeerReadMessage msg, Host from, short sProto, int channel);

    protected abstract void onPeerReadResponseMessage(PeerReadResponseMessage msg,
                                                      Host from, short sProto, int channel);

    protected abstract void onPeerWriteResponseMessage(PeerWriteResponseMessage msg,
                                                       Host from, short sProto, int channel);

    protected abstract void onPeerWriteMessage(PeerWriteMessage msg, Host from, short sProto, int channel);

    protected abstract void onOutConnectionUp(OutConnectionUp event, int channel);

    protected abstract void onOutConnectionDown(OutConnectionDown event, int channel);

    protected abstract void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel);

    private void onInConnectionDown(InConnectionDown event, int channel) {
        logger.debug(event);
    }

    /* ------------------------------------------- ------------- ------------------------------------------- */
    /* ------------------------------------------- CONSENSUS OPS ------------------------------------------- */
    /* ------------------------------------------- ------------- ------------------------------------------- */

    private void onInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void onInstallSnapshot(InstallSnapshotNotification not, short from) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(not.getState());
            DataInputStream in = new DataInputStream(bais);
            nWrites = in.readInt();
            incrementalHash = new byte[in.readInt()];
            int read = in.read(incrementalHash);
            assert read == incrementalHash.length;
            StringBuilder sb = new StringBuilder();
            for (byte b : incrementalHash)
                sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            logger.info("State installed(" + nWrites + ") " + sb.toString());
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new AssertionError();
        }
    }

    public void onGetStateSnapshot(GetSnapshotRequest not, short from) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(nWrites);
            out.writeInt(incrementalHash.length);
            out.write(incrementalHash);
            StringBuilder sb = new StringBuilder();
            for (byte b : incrementalHash)
                sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            logger.debug("State stored(" + nWrites + ") " + sb.toString());
            sendReply(new DeliverSnapshotReply(not.getSnapshotTarget(),
                    not.getSnapshotInstance(), baos.toByteArray()), from);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new AssertionError();
        }
    }

    //int counter = 0;
    private void onExecuteBatch(ExecuteBatchNotification reply, short from) {
        //counter++;
        //if(counter % 10000 == 0)
        //    logger.info("State: " + Arrays.toString(incrementalHash));
        //incrementalHash = sha1(incrementalHash, reply.getBatch().getBatchId());
        nWrites += reply.getBatch().getOps().size();

        _onExecuteBatch(reply, from);
    }

    protected abstract void _onExecuteBatch(ExecuteBatchNotification reply, short from);

    private void onExecuteRead(ExecuteReadReply reply, short from) {
        _onExecuteRead(reply, from);
    }

    protected abstract void _onExecuteRead(ExecuteReadReply reply, short from);

    //Utils

    protected abstract void onMembershipChange(MembershipChange notification, short emitterId);

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

}
