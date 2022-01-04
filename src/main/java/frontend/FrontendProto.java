package frontend;

import app.Application;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import frontend.ipc.DeliverSnapshotReply;
import frontend.ipc.GetSnapshotRequest;
import frontend.network.*;
import frontend.notifications.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

public abstract class FrontendProto extends GenericProtocol {

    public static final String ADDRESS_KEY = "frontend_address";
    public static final String PEER_PORT_KEY = "frontend_peer_port";
    private static final Logger logger = LogManager.getLogger(FrontendProto.class);
    protected final int PEER_PORT;
    protected final InetAddress self;
    protected final Application app;
    private final int opPrefix;
    private final short protoIndex;
    protected int peerChannel;
    protected List<InetAddress> membership;
    private int opCounter;

    public FrontendProto(String protocolName, short protocolId, Properties props,
                         short protoIndex, Application app) throws IOException {
        super(protocolName, protocolId);

        this.app = app;
        this.PEER_PORT = Integer.parseInt(props.getProperty(PEER_PORT_KEY)) + protoIndex;

        self = InetAddress.getByName(props.getProperty(ADDRESS_KEY));
        opPrefix = ByteBuffer.wrap(self.getAddress()).getInt();
        opCounter = 0;
        membership = null;

        this.protoIndex = protoIndex;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        //Peer
        Properties peerProps = new Properties();
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, Integer.toString(PEER_PORT));
        //peerProps.put(TCPChannel.DEBUG_INTERVAL_KEY, 10000);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        registerMessageSerializer(peerChannel, PeerBatchMessage.MSG_CODE, PeerBatchMessage.serializer);
        registerMessageHandler(peerChannel, PeerBatchMessage.MSG_CODE, this::onPeerBatchMessage, this::uponMessageFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);

        //Consensus
        subscribeNotification(MembershipChange.NOTIFICATION_ID, this::onMembershipChange);
        subscribeNotification(ExecuteBatchNotification.NOTIFICATION_ID, this::onExecuteBatch);
        subscribeNotification(InstallSnapshotNotification.NOTIFICATION_ID, this::onInstallSnapshot);
        registerRequestHandler(GetSnapshotRequest.REQUEST_ID, this::onGetStateSnapshot);
        _init(props);
    }

    protected abstract void _init(Properties props) throws HandlerRegistrationException;

    protected long nextId() {
        //Message id is constructed using the server ip and a local counter (makes it unique and sequential)
        opCounter++;
        return ((long) opCounter << 32) | (opPrefix & 0xFFFFFFFFL);
    }

    /* ----------------------------------------------- ------------- ------------------------------------------ */
    /* ----------------------------------------------- APP INTERFACE ------------------------------------------ */
    /* ----------------------------------------------- ------------- ------------------------------------------ */

    //TODO SYNCHRONIZE THIS FOR EVERY FRONTEND
    public abstract void submitOperation(byte[] op, OpType type);

    /* ----------------------------------------------- ----------- ----------------------------------------------- */
    /* ----------------------------------------------- PEER EVENTS ----------------------------------------------- */
    /* ----------------------------------------------- ----------- ----------------------------------------------- */

    protected abstract void onPeerBatchMessage(PeerBatchMessage msg, Host from, short sProto, int channel);

    protected abstract void onOutConnectionUp(OutConnectionUp event, int channel);

    protected abstract void onOutConnectionDown(OutConnectionDown event, int channel);

    protected abstract void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel);

    private void onInConnectionDown(InConnectionDown event, int channel) {
        logger.debug(event);
    }

    private void onInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    /* ------------------------------------------- ------------- ------------------------------------------- */
    /* ------------------------------------------- CONSENSUS OPS ------------------------------------------- */
    /* ------------------------------------------- ------------- ------------------------------------------- */

    private void onInstallSnapshot(InstallSnapshotNotification not, short from) {
        app.installState(not.getState());
    }

    public void onGetStateSnapshot(GetSnapshotRequest not, short from) {
        byte[] state = app.getSnapshot();
        sendReply(new DeliverSnapshotReply(not.getSnapshotTarget(),
                not.getSnapshotInstance(), state), from);
    }

    protected abstract void onExecuteBatch(ExecuteBatchNotification reply, short from);

    protected abstract void onMembershipChange(MembershipChange notification, short emitterId);

    public void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    public enum OpType {STRONG_READ, WRITE}

}
