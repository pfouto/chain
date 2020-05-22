package frontend;

import babel.exceptions.HandlerRegistrationException;
import babel.generic.GenericProtocol;
import babel.generic.ProtoMessage;
import channel.simpleclientserver.SimpleServerChannel;
import channel.simpleclientserver.events.ClientDownEvent;
import channel.simpleclientserver.events.ClientUpEvent;
import channel.tcp.MultithreadedTCPChannel;
import channel.tcp.events.*;
import frontend.ops.ReadOp;
import frontend.ops.WriteBatch;
import frontend.notifications.*;
import frontend.network.*;
import frontend.timers.BatchTimer;
import io.netty.channel.EventLoopGroup;
import network.data.Host;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class FrontendProto extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(FrontendProto.class);

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Frontend";

    public static final String ADDRESS_KEY = "frontend_address";
    public static final String PEER_PORT_KEY = "frontend_peer_port";
    public static final String SERVER_PORT_KEY = "frontend_server_port";
    public static final String READ_RESPONSE_BYTES_KEY = "read_response_bytes";
    public static final String BATCH_INTERVAL_KEY = "batch_interval";

    private final int READ_RESPONSE_BYTES;
    private final int PEER_PORT;
    private final int BATCH_INTERVAL;

    private final InetAddress self;
    private int serverChannel;
    private int peerChannel;

    private List<InetAddress> membership;
    private Host readsTo;
    private Host writesTo;
    private InetAddress responder;

    private final int opPrefix;
    private int opCounter;

    private final Map<Long, List<Pair<Host, Integer>>> idMapper;

    //ToForward
    private final List<Pair<Host, RequestMessage>> batchBuffer;
    //Forwarded
    private final Map<Long, ReadOp> pendingReads;
    private final Map<Long, WriteBatch> pendingWrites;

    //App state
    private int nWrites;
    private byte[] incrementalHash;

    private final EventLoopGroup workerGroup;

    private final byte[] response;

    public FrontendProto(Properties props, EventLoopGroup workerGroup) throws IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.READ_RESPONSE_BYTES = Integer.parseInt(props.getProperty(READ_RESPONSE_BYTES_KEY));
        this.PEER_PORT = Integer.parseInt(props.getProperty(PEER_PORT_KEY));
        this.BATCH_INTERVAL = Integer.parseInt(props.getProperty(BATCH_INTERVAL_KEY));
        this.workerGroup = workerGroup;
        self = InetAddress.getByName(props.getProperty(ADDRESS_KEY));
        opPrefix = ByteBuffer.wrap(self.getAddress()).getInt();
        response = new byte[READ_RESPONSE_BYTES];
        opCounter = 0;
        responder = null;
        readsTo = null;
        writesTo = null;
        batchBuffer = new LinkedList<>();
        pendingReads = new HashMap<>();
        pendingWrites = new HashMap<>();
        idMapper = new HashMap<>();
        membership = null;

        nWrites = 0;
        incrementalHash = new byte[0];
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        setupPeriodicTimer(new BatchTimer(), BATCH_INTERVAL, BATCH_INTERVAL);
        registerTimerHandler(BatchTimer.TIMER_ID, this::handleBatchTimer);

        //Server
        Properties serverProps = new Properties();
        serverProps.put(SimpleServerChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        serverProps.put(SimpleServerChannel.PORT_KEY, props.getProperty(SERVER_PORT_KEY));
        serverProps.put(SimpleServerChannel.WORKER_GROUP_KEY, workerGroup);
        serverChannel = createChannel(SimpleServerChannel.NAME, serverProps);
        registerMessageSerializer(RequestMessage.MSG_CODE, RequestMessage.serializer);
        registerMessageSerializer(ResponseMessage.MSG_CODE, ResponseMessage.serializer);
        registerMessageHandler(serverChannel, RequestMessage.MSG_CODE, this::onRequestMessage);
        registerMessageHandler(serverChannel, ResponseMessage.MSG_CODE, null, this::onReplyMessageFail);
        registerChannelEventHandler(serverChannel, ClientUpEvent.EVENT_ID, this::onClientUp);
        registerChannelEventHandler(serverChannel, ClientDownEvent.EVENT_ID, this::onClientDown);

        //Peer
        Properties peerProps = new Properties();
        peerProps.put(MultithreadedTCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.put(MultithreadedTCPChannel.PORT_KEY, props.getProperty(PEER_PORT_KEY));
        peerProps.put(MultithreadedTCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(MultithreadedTCPChannel.NAME, peerProps);
        registerMessageSerializer(PeerReadMessage.MSG_CODE, PeerReadMessage.serializer);
        registerMessageSerializer(PeerWriteMessage.MSG_CODE, PeerWriteMessage.serializer);
        registerMessageSerializer(PeerReadResponseMessage.MSG_CODE, PeerReadResponseMessage.serializer);
        registerMessageSerializer(PeerWriteResponseMessage.MSG_CODE, PeerWriteResponseMessage.serializer);
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
        subscribeNotification(InstallSnapshotNotification.NOTIFICATION_ID, this::onInstallSnapshot);
        subscribeNotification(GetSnapshotNotification.NOTIFICATION_ID, this::onGetStateSnapshot);

    }

    private long nextId() {
        //Message id is constructed using the server ip and a local counter (makes it unique and sequential)
        //TODO test if results in incrementing ids
        opCounter++;
        return ((long) opCounter << 32) | (opPrefix & 0xFFFFFFFFL);
    }

    /* -------------------- ---------- ----------------------------------------------- */
    /* -------------------- CLIENT OPS ----------------------------------------------- */
    /* -------------------- ---------- ----------------------------------------------- */

    private void onClientUp(ClientUpEvent event, int channel) {
        logger.debug(event);
    }

    private void onClientDown(ClientDownEvent event, int channel) {
        logger.debug(event);
    }

    private void onRequestMessage(RequestMessage msg, Host from, short sProto, int channel) {
        if (msg.getOpType() == RequestMessage.READ) {
            long internalId = nextId();
            idMapper.put(internalId, Collections.singletonList(Pair.of(from, msg.getOpId())));
            ReadOp op = new ReadOp(internalId, msg.getPayload());
            pendingReads.put(internalId, op);
            sendPeerReadMessage(new PeerReadMessage(op), readsTo);
        } else {
            batchBuffer.add(Pair.of(from, msg));
        }
    }

    private void onReplyMessageFail(ResponseMessage message, Host host, short dProto, Throwable cause, int channel) {
        logger.warn(message + " failed to " + host + " - " + cause);
    }

    /* -------------------- -------- ----------------------------------------------- */
    /* -------------------- PEER OPS ----------------------------------------------- */
    /* -------------------- -------- ----------------------------------------------- */

    private void onPeerReadMessage(PeerReadMessage msg, Host from, short sProto, int channel) {
        sendPeerReadResponseMessage(
                new PeerReadResponseMessage(new ReadOp(msg.getOp().getOpId(), response)), from);
    }

    private void onPeerReadResponseMessage(PeerReadResponseMessage msg, Host from, short sProto, int channel) {
        Pair<Host, Integer> pair = idMapper.get(msg.getOp().getOpId()).get(0);
        sendMessage(serverChannel, new ResponseMessage(pair.getRight(), ResponseMessage.READ, msg.getOp().getOpData()),
                pair.getKey());
    }

    private void onPeerWriteResponseMessage(PeerWriteResponseMessage msg, Host from, short sProto, int channel) {
        List<Pair<Host, Integer>> ops = idMapper.remove(msg.getBatchId());
        if (ops == null) {
            logger.error("No entry in idMapper");
            throw new AssertionError("No entry in idMapper");
        }
        ops.forEach(p -> sendMessage(serverChannel, new ResponseMessage(p.getValue(),
                RequestMessage.WRITE, new byte[0]), p.getKey()));
    }

    private void onPeerWriteMessage(PeerWriteMessage msg, Host from, short sProto, int channel) {
        triggerNotification(new SubmitBatchNotification(msg.getBatch()));
    }

    private void handleBatchTimer(BatchTimer timer, long l) {
        if (batchBuffer.isEmpty()) return;
        long internalId = nextId();
        List<Pair<Host, Integer>> opIds = new ArrayList<>(batchBuffer.size());
        List<byte[]> ops = new ArrayList<>(batchBuffer.size());
        batchBuffer.forEach(p -> {
            opIds.add(Pair.of(p.getLeft(), p.getRight().getOpId()));
            ops.add(p.getRight().getPayload());
        });
        batchBuffer.clear();
        List<Pair<Host, Integer>> put = idMapper.put(internalId, opIds);
        if (put != null) {
            logger.error("Duplicate internalId");
            throw new AssertionError("Duplicate internalId");
        }
        WriteBatch batch = new WriteBatch(internalId, self, ops);
        pendingWrites.put(internalId, batch);
        sendPeerWriteMessage(new PeerWriteMessage(batch), writesTo);
    }

    private void onOutConnectionUp(OutConnectionUp event, int channel) {
        //logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.debug("Connected to writesTo " + peer);
        } else if (peer.equals(readsTo)) {
            logger.debug("Connected to readsTo " + peer);
        } else if (!responder.equals(self)){
            logger.warn("Unexpected connectionUp, ignoring and closing: " + event);
            closeConnection(peer, peerChannel);
        }
    }

    private void onOutConnectionDown(OutConnectionDown event, int channel) {
        //logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Lost connection to writesTo, re-connecting: " + event);
            connectAndSendPendingToWritesTo();
        } else if (peer.equals(readsTo)) {
            logger.warn("Lost connection to readsTo, re-connecting: " + event);
            connectAndSendPendingToReadsTo();
        }
    }

    private void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(writesTo)) {
            logger.warn("Connection failed to writesTo, re-trying: " + event);
            connectAndSendPendingToWritesTo();
        } else if (peer.equals(readsTo)) {
            logger.warn("Connection failed to readsTo, re-trying: " + event);
            connectAndSendPendingToReadsTo();
        }
    }

    private void connectAndSendPendingToWritesTo() {
        if (!writesTo.getAddress().equals(self))
            openConnection(writesTo, peerChannel);
        pendingWrites.values().forEach(b -> sendPeerWriteMessage(new PeerWriteMessage(b), writesTo));
    }

    private void connectAndSendPendingToReadsTo() {
        if (!readsTo.getAddress().equals(self))
            openConnection(readsTo, peerChannel);
        pendingReads.values().forEach(r -> sendPeerReadMessage(new PeerReadMessage(r), readsTo));
    }

    private void onInConnectionDown(InConnectionDown event, int channel) {
        logger.debug(event);
    }

    private void onInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void sendPeerWriteMessage(PeerWriteMessage msg, Host destination) {
        if (destination.getAddress().equals(self)) onPeerWriteMessage(msg, destination, getProtoId(), peerChannel);
        else sendMessage(peerChannel, msg, destination);
    }

    private void sendPeerReadMessage(PeerReadMessage msg, Host destination) {
        if (destination.getAddress().equals(self)) onPeerReadMessage(msg, destination, getProtoId(), peerChannel);
        else sendMessage(peerChannel, msg, destination);
    }

    private void sendPeerReadResponseMessage(PeerReadResponseMessage msg, Host destination) {
        if (destination.getAddress().equals(self))
            onPeerReadResponseMessage(msg, destination, getProtoId(), peerChannel);
        else sendMessage(peerChannel, msg, destination, MultithreadedTCPChannel.CONNECTION_IN);
    }

    private void sendPeerWriteResponseMessage(PeerWriteResponseMessage msg, Host destination) {
        if (destination.getAddress().equals(self)) {
            onPeerWriteResponseMessage(msg, destination, getProtoId(), peerChannel);
        } else {
            sendMessage(peerChannel, msg, destination);
        }
    }

    /* -------------------- ------------- ----------------------------------------------- */
    /* -------------------- CONSENSUS OPS ----------------------------------------------- */
    /* -------------------- ------------- ----------------------------------------------- */

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

    public void onGetStateSnapshot(GetSnapshotNotification not, short from) {
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
            triggerNotification(new DeliverSnapshotNotification(not.getSnapshotTarget(),
                    not.getSnapshotInstance(), baos.toByteArray()));
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new AssertionError();
        }
    }

    int counter = 0;

    private void onExecuteBatch(ExecuteBatchNotification reply, short from) {
        //counter++;
        //if(counter % 5000 == 0)
        //    logger.info("State: " + Arrays.toString(incrementalHash));
        nWrites += reply.getBatch().getOps().size();
        //incrementalHash = sha1(incrementalHash, reply.getBatch().getBatchId());
        if (self.equals(responder) || (responder == null && reply.getBatch().getIssuer().equals(self))) {
            sendPeerWriteResponseMessage(new PeerWriteResponseMessage(reply.getBatch().getBatchId()),
                    new Host(reply.getBatch().getIssuer(), PEER_PORT));
        }
    }

    private void onMembershipChange(MembershipChange notification, short emitterId) {

        //Stopped being responder
        if (self.equals(responder) && !notification.getResponder().equals(self)) {
            //Close to everyone from old membership, except new writesTo and readsTo
            membership.stream().filter(h ->
                    !h.equals(self) && !h.equals(notification.getReadsTo()) && !h.equals(notification.getWritesTo()))
                    .forEach(h -> closeConnection(new Host(h, PEER_PORT), peerChannel));
        }
        //update membership and responder
        membership = notification.getOrderedMembers();
        responder = notification.getResponder();

        //Reads to changed
        if (readsTo == null || !notification.getReadsTo().equals(readsTo.getAddress())) {
            //Close old readsTo
            if (readsTo != null && !readsTo.getAddress().equals(self))
                closeConnection(readsTo, peerChannel);
            //Update and open to new readsTo
            readsTo = new Host(notification.getReadsTo(), PEER_PORT);
            logger.info("New readsTo: " + readsTo.getAddress());
            connectAndSendPendingToReadsTo();
        }

        //Writes to changed
        if (writesTo == null || !notification.getWritesTo().equals(writesTo.getAddress())) {
            //Close old writesTo
            if (writesTo != null && !writesTo.getAddress().equals(self))
                closeConnection(writesTo, peerChannel);
            //Update and open to new writesTo
            writesTo = new Host(notification.getWritesTo(), PEER_PORT);
            logger.info("New writesTo: " + writesTo.getAddress());
            connectAndSendPendingToWritesTo();
        }

        //Started being responder
        if (self.equals(responder)) {
            logger.info("Am responder.");
            //Open to everyone except writesTo and readsTo (already open on previous step)
            notification.getOrderedMembers().stream().
                    filter(h -> !h.equals(self) && !h.equals(writesTo.getAddress()) && !h.equals(readsTo.getAddress())).
                    forEach(h -> openConnection(new Host(h, PEER_PORT), peerChannel));
        }
    }

    //Utils

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

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

}
