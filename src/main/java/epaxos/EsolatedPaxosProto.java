package epaxos;

import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.MultithreadedTCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import common.values.AppOpBatch;
import common.values.PaxosValue;
import epaxos.messages.*;
import epaxos.timers.ReconnectTimer;
import epaxos.utils.GraphNode;
import epaxos.utils.Instance;
import epaxos.utils.Membership;
import frontend.notifications.ExecuteBatchNotification;
import frontend.notifications.MembershipChange;
import frontend.ipc.SubmitBatchRequest;
import frontend.timers.InfoTimer;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static epaxos.utils.Instance.InstanceState.*;

public class EsolatedPaxosProto extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(EsolatedPaxosProto.class);

    public final static short PROTOCOL_ID = 500;
    public final static String PROTOCOL_NAME = "EsolatedPaxos";

    private static final int INITIAL_MAP_SIZE = 1000;

    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";
    public static final String MAX_CONCURRENT_FAILS_KEY = "max_concurrent_fails";
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    public static final String INITIAL_MEMBERSHIP_KEY = "initial_membership";

    private final int MAX_CONCURRENT_FAILS;
    private final int RECONNECT_TIME;

    private final Host self;
    private final Membership membership;

    private final Map<Host, ConcurrentMap<Integer, Instance>> instances = new ConcurrentHashMap<>();
    //Could use treeMap instead of this... but hashMap has better performance (...?)
    private final Map<Host, Integer> highestReceivedInstance = new HashMap<>();
    private final Map<Host, Integer> highestExecutedInstance = new HashMap<>(); //Need this one either way

    private int lastStartedInstance;
    private final int currentEpoch;
    private final LinkedList<Host> seeds;

    private final EventLoopGroup workerGroup;
    private int peerChannel;

    public EsolatedPaxosProto(Properties props, EventLoopGroup workerGroup) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.workerGroup = workerGroup;

        this.lastStartedInstance = -1;
        this.currentEpoch = 0;

        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));

        RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));
        MAX_CONCURRENT_FAILS = Integer.parseInt(props.getProperty(MAX_CONCURRENT_FAILS_KEY));

        seeds = readSeeds(props.getProperty(INITIAL_MEMBERSHIP_KEY));
        if (seeds.contains(self))
            membership = new Membership(self, seeds, MAX_CONCURRENT_FAILS);
        else
            throw new AssertionError("Non seed starting in active state");

        seeds.forEach(h -> instances.put(h, new ConcurrentHashMap<>(INITIAL_MAP_SIZE)));
        seeds.forEach(h -> highestExecutedInstance.put(h, -1));

    }


    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        Properties peerProps = new Properties();
        peerProps.put(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        peerProps.put(TCPChannel.WORKER_GROUP_KEY, workerGroup);
        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        setDefaultChannel(peerChannel);

        registerMessageSerializer(peerChannel, AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptOkMsg.MSG_CODE, AcceptOkMsg.serializer);
        registerMessageSerializer(peerChannel, CommitMsg.MSG_CODE, CommitMsg.serializer);
        registerMessageSerializer(peerChannel, PreAcceptMsg.MSG_CODE, PreAcceptMsg.serializer);
        registerMessageSerializer(peerChannel, PreAcceptOkMsg.MSG_CODE, PreAcceptOkMsg.serializer);

        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptOkMsg.MSG_CODE, this::uponAcceptOkMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, CommitMsg.MSG_CODE, this::uponCommitMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PreAcceptMsg.MSG_CODE, this::uponPreAcceptMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PreAcceptOkMsg.MSG_CODE, this::uponPreAcceptOkMsg, this::uponMessageFailed);

        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);

        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);

        seeds.stream().filter(h -> !h.equals(self)).forEach(this::openConnection);

        new Thread(this::auxiliaryLoop, "Executor Thread").start();

        triggerMembershipChangeNotification();

        logger.info("EsolatedPaxos: " + membership + " mcf " + MAX_CONCURRENT_FAILS);
    }

    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        startNextInstance(new AppOpBatch(not.getBatch()));
    }

    private void startNextInstance(PaxosValue nextValue) {

        if (nextValue.type == PaxosValue.Type.MEMBERSHIP) {
            logger.error("Membership op: " + nextValue);
            throw new AssertionError("Membership op: " + nextValue);
        }

        int instN = ++lastStartedInstance;
        Map<Host, Integer> deps = new HashMap<>();
        int seqNumber = -1;

        Integer highestInstNumber = highestReceivedInstance.get(self);
        if (highestInstNumber != null) {
            Instance highestInst = instances.get(self).get(highestInstNumber);
            assert highestInst != null;
            assert highestInst.getState() != PLACEHOLDER;
            deps.put(self, highestInstNumber);
            seqNumber = Math.max(highestInst.getSeqNumber(), seqNumber);
        }

        seqNumber++;

        Instance instance = new Instance(self, instN, currentEpoch, self);
        instance.preAccept(nextValue, seqNumber, deps);
        Instance put = instances.get(self).put(instN, instance);
        assert put == null;

        PreAcceptMsg msg = new PreAcceptMsg(instance.getBallot(), nextValue, seqNumber, deps, self, instN);
        Integer put1 = highestReceivedInstance.put(self, instN);
        assert put1 == null || put1 < instN;
        membership.getFastPathReplicasExcludingSelf().forEachRemaining(h -> sendMessage(msg, h));
    }

    private void uponPreAcceptMsg(PreAcceptMsg msg, Host from, short sourceProto, int channel) {
        assert !from.equals(self);
        assert from.equals(msg.replica);

        Map<Host, Integer> deps = msg.deps;
        int seqNumber = msg.sN;
        //Update deps/sN
        ConcurrentMap<Integer, Instance> entry = instances.get(from);
        Integer highestInstNumber = highestReceivedInstance.get(from);

        if (highestInstNumber != null && highestInstNumber > deps.getOrDefault(from, -1)) {
            Instance highestInst = entry.get(highestInstNumber);
            assert highestInst != null;
            assert highestInst.getState() != PLACEHOLDER;
            deps.put(from, highestInstNumber);
            seqNumber = Math.max(highestInst.getSeqNumber() + 1, seqNumber);
        }

        Instance inst = instances.get(msg.replica).computeIfAbsent(msg.iN,
                k -> new Instance(msg.replica, msg.iN, msg.ballot));
        inst.preAccept(msg.value, seqNumber, deps);
        highestReceivedInstance.merge(msg.replica, msg.iN, Math::max);
        sendMessage(new PreAcceptOkMsg(inst.getBallot(), msg.value, seqNumber, deps, msg.replica, msg.iN), from);
    }

    private void uponPreAcceptOkMsg(PreAcceptOkMsg msg, Host from, short sourceProto, int channel) {
        Instance inst = instances.get(msg.replica).get(msg.instanceNumber);
        assert inst != null;
        if (inst.getState() != PREACCEPTED) return;
        inst.registerPreAcceptOkMessage(from, msg);

        if (inst.getPreAcceptOkMsgs().size() == membership.getFastPathSizeExcludingSelf()) {
            AcceptMsg acceptMsg = maybeGenerateAcceptMessage(inst.getPreAcceptOkMsgs().values());
            if (acceptMsg != null) {
                inst.accept(acceptMsg.value, acceptMsg.seqNumber, acceptMsg.deps);
                assert acceptMsg.ballot.b == 0;
                membership.getMajorityQuorumReplicasExcludingSelf().forEachRemaining(h -> sendMessage(acceptMsg, h));
            } else { //All messages equal
                commit(msg.value, msg.seqNumber, msg.deps, msg.replica, msg.instanceNumber);
            }
        }
    }

    private AcceptMsg maybeGenerateAcceptMessage(Collection<PreAcceptOkMsg> msg) {
        Iterator<PreAcceptOkMsg> iterator = msg.iterator();
        PreAcceptOkMsg next = iterator.next();
        Map<Host, Integer> deps = new HashMap<>(next.deps);
        int seqNumber = next.seqNumber;
        boolean merged = false;
        while (iterator.hasNext()) {
            next = iterator.next();
            if (next.deps.size() != deps.size()) merged = true;
            for (Map.Entry<Host, Integer> e : next.deps.entrySet()) {
                Integer current = deps.getOrDefault(e.getKey(), -1);
                if (!e.getValue().equals(current)) {
                    deps.put(e.getKey(), Math.max(e.getValue(), current));
                    merged = true;
                }
            }
            if (next.seqNumber != seqNumber) seqNumber = Math.max(next.seqNumber, seqNumber);
        }
        return merged ? new AcceptMsg(next.ballot, next.value, seqNumber, deps, next.replica,
                next.instanceNumber) : null;
    }

    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        Instance inst = instances.get(msg.replica).computeIfAbsent(msg.instanceNumber,
                k -> new Instance(msg.replica, msg.instanceNumber, msg.ballot));
        inst.accept(msg.value, msg.seqNumber, msg.deps);
        highestReceivedInstance.merge(msg.replica, msg.instanceNumber, Math::max);
        sendMessage(new AcceptOkMsg(msg.ballot, msg.value, msg.replica, msg.instanceNumber), from);
    }

    private void uponAcceptOkMsg(AcceptOkMsg msg, Host from, short sourceProto, int channel) {
        Instance inst = instances.get(msg.replica).get(msg.instanceNumber);
        assert inst != null;
        if (inst.getState() != ACCEPTED) return;
        inst.registerAcceptOkMessage(from, msg);
        if (inst.getAcceptOkMsgs().size() == membership.getMajorityQuorumSizeExcludingSelf()) {
            commit(inst.getValue(), inst.getSeqNumber(), inst.getDeps(), msg.replica, msg.instanceNumber);
        }
    }

    private void commit(PaxosValue value, int seqNumber, Map<Host, Integer> deps, Host replica, int instanceNumber) {
        Instance inst = instances.get(replica).get(instanceNumber);
        assert inst != null;
        inst.commit(value, seqNumber, deps);
        CommitMsg msg = new CommitMsg(inst.getBallot(), value, seqNumber, deps, replica, instanceNumber);
        membership.allExceptMe().forEachRemaining(h -> sendMessage(msg, h));
    }

    private void uponCommitMsg(CommitMsg msg, Host from, short sourceProto, int channel) {
        Instance inst = instances.get(msg.replica)
                .computeIfAbsent(msg.instanceNumber,
                        k -> new Instance(msg.replica, msg.instanceNumber, msg.ballot));
        inst.commit(msg.value, msg.seqNumber, msg.deps);
        highestReceivedInstance.merge(msg.replica, msg.instanceNumber, Math::max);
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channel) {
        logger.warn(event);
        if (membership.contains(event.getNode())) {
            setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> ev, int ch) {
        logger.warn("Connection failed to " + ev.getNode() + ", cause: " + ev.getCause().getMessage());
        if (membership.contains(ev.getNode()))
            setupTimer(new ReconnectTimer(ev.getNode()), RECONNECT_TIME);
    }

    private void onReconnectTimer(ReconnectTimer timer, long timerId) {
        if (membership.contains(timer.getHost()))
            openConnection(timer.getHost());
    }

    private void uponInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void uponInConnectionDown(InConnectionDown event, int channel) {
        logger.info(event);
    }

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    private LinkedList<Host> readSeeds(String membershipProp) throws UnknownHostException {
        //read initial seeds
        LinkedList<Host> peers = new LinkedList<>();
        String[] initialMembership = membershipProp.split(",");
        for (String s : initialMembership) {
            peers.add(new Host(InetAddress.getByName(s), self.getPort()));
        }
        return peers;
    }

    private void triggerMembershipChangeNotification() {
        triggerNotification(new MembershipChange(
                membership.getMembers().stream().map(Host::getAddress).collect(Collectors.toList()),
                null, self.getAddress(), null));
    }


    //Auxiliary thread methods
    private void auxiliaryLoop() {
        while (true) {
            boolean executed = false;
            Set<Host> hosts = new HashSet<>(highestExecutedInstance.keySet());
            for (Host host : hosts) {
                int nextInst = highestExecutedInstance.get(host) + 1;
                Instance inst = instances.get(host).get(nextInst);
                if (inst == null || inst.getState() != COMMITTED) continue;

                int size = maybeExecute(host, nextInst).size();
                if (size > 0)
                    executed = true;
            }
            if (!executed) {
                //try {
                //    Thread.sleep(1);
                //} catch (InterruptedException ignored) {
                //}
            }
        }
    }

    private Set<Map.Entry<Host, Integer>> maybeExecute(Host replica, int instNumber) {
        Instance inst = instances.get(replica).get(instNumber);
        assert inst.getState() == COMMITTED;
        assert !inst.isExecuted();

        GraphNode graphNode = new GraphNode(replica, instNumber);
        Map<Map.Entry<Host, Integer>, GraphNode> allNodes = new HashMap<>();
        allNodes.put(new AbstractMap.SimpleEntry<>(replica, instNumber), graphNode);
        Set<Map.Entry<Host, Integer>> executedInsts = new HashSet<>();
        strongConnect(graphNode, new BoxedInteger(), new Stack<>(), allNodes, executedInsts);
        return executedInsts;
    }

    private boolean strongConnect(GraphNode v, BoxedInteger index, Stack<GraphNode> stack,
                                  Map<Map.Entry<Host, Integer>, GraphNode> allNodes,
                                  Set<Map.Entry<Host, Integer>> executedInsts) {
        v.index = index.getValue();
        v.lowLink = index.getValue();
        index.inc();
        stack.push(v);
        v.onStack = true;

        List<GraphNode> depNodes = new ArrayList<>();
        //Check if deps are executed or not yet committed...
        Instance instance = instances.get(v.replica).get(v.iN);
        if (instance == null) throw new AssertionError("instance null");

        for (Map.Entry<Host, Integer> e : instance.getDeps().entrySet()) {
            Instance inst = instances.get(e.getKey()).get(e.getValue());
            if (inst == null || inst.getState() != COMMITTED)
                return false;

            if (!inst.isExecuted()) {
                GraphNode graphNode = allNodes.computeIfAbsent(new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()),
                        k -> new GraphNode(e.getKey(), e.getValue()));
                depNodes.add(graphNode);
            }
        }
        for (GraphNode w : depNodes) {
            if (w.index == -1) {
                boolean success = strongConnect(w, index, stack, allNodes, executedInsts);
                if (!success) return false;
                v.lowLink = Math.min(v.lowLink, w.lowLink);
            } else if (w.onStack) {
                v.lowLink = Math.min(v.lowLink, w.index);
            }
        }
        if (v.lowLink == v.index) {
            GraphNode w;
            //Start strongly connected component
            List<Instance> ssc = new LinkedList<>();
            do {
                w = stack.pop();
                w.onStack = false;
                ssc.add(instances.get(w.replica).get(w.iN));
            } while (!v.equals(w));
            //End strongly connected component
            ssc.sort(Comparator.comparing(Instance::getSeqNumber)
                    .thenComparing(Instance::getReplica)
                    .thenComparingInt(Instance::getiN));
            for (Instance i : ssc) {
                executedInsts.add(new AbstractMap.SimpleEntry<>(i.getReplica(), i.getiN()));
                execute(i);
                highestExecutedInstance.merge(i.getReplica(), i.getiN(), Math::max);
            }
        }
        return true;
    }

    private void execute(Instance instance) {
        instance.markExecuted();
        logger.debug("Executed: " + instance.getReplica() + ":" + instance.getiN() + " - " + instance.getValue());
        if (instance.getValue().type == PaxosValue.Type.APP_BATCH)
            triggerNotification(new ExecuteBatchNotification(((AppOpBatch) instance.getValue()).getBatch()));
        else
            throw new AssertionError("Trying to execute unknown paxos value: " + instance.getValue());
    }

    static class BoxedInteger {
        private int value;

        BoxedInteger() {
            value = 0;
        }

        int getValue() {
            return value;
        }

        void inc() {
            value++;
        }
    }


}
