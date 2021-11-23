package chainreplication.zookeeper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessNode implements Runnable {

    private static final String LEADER_ELECTION_ROOT_NODE = "/membership";
    private static final String SN_ROOT_NODE = "/sn";
    private static final String PROCESS_NODE_PREFIX = "/p_";

    private final ZooKeeperService zooKeeperService;

    private int myId = -1;
    private final Host self;
    private final Map<Integer, Host> nodeMap = new HashMap<>();

    private IMembershipListener list;

    public ProcessNode(final String zkURL, Host self, IMembershipListener list) throws IOException {
        this.list = list;
        this.self = self;
        zooKeeperService = new ZooKeeperService(zkURL, new ProcessNodeWatcher());
    }

    public Host getNodeAddress(int nodeId) {
        Host addr = nodeMap.get(nodeId);
        if (addr != null) {
            return addr;
        } else {
            Host h;
            byte[] nodeData = zooKeeperService.getNodeData(
                    LEADER_ELECTION_ROOT_NODE + PROCESS_NODE_PREFIX + String.format("%010d", nodeId),
                    false);
            try {
                h = Host.serializer.deserialize(Unpooled.buffer().writeBytes(nodeData));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            nodeMap.put(nodeId, h);
            return h;
        }
    }

    public List<Host> getAddresses(List<Integer> ids){
        return ids.stream().map(this::getNodeAddress).collect(Collectors.toList());
    }

    @Override
    public void run() {
        final String rootNodePath = zooKeeperService.createNode(LEADER_ELECTION_ROOT_NODE, new byte[0], false, false);
        if (rootNodePath == null) {
            throw new IllegalStateException(
                    "Unable to create/access leader election root node with path: " + LEADER_ELECTION_ROOT_NODE);
        }
        final String snNodePath = zooKeeperService.createNode(SN_ROOT_NODE, new byte[0], false, false);
        if (snNodePath == null) {
            throw new IllegalStateException(
                    "Unable to create/access sn root node with path: " + SN_ROOT_NODE);
        }
        membershipChanged();
    }

    public void registerSelf() throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        Host.serializer.serialize(self, buffer);
        buffer.writeInt(-1);
        buffer.writeInt(-1);
        String myNode = zooKeeperService.createNode(LEADER_ELECTION_ROOT_NODE + PROCESS_NODE_PREFIX, buffer.array(),
                                                    false, true);
        if (myNode == null) {
            throw new IllegalStateException(
                    "Unable to create/access process node with path: " + LEADER_ELECTION_ROOT_NODE);
        }
        myId = Integer.parseInt(myNode.substring(myNode.lastIndexOf("_") + 1));
    }

    public void publishSN(int deadNode, int sn) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(sn);
        String node = zooKeeperService.createNode(SN_ROOT_NODE + "/" + myId + "_" + deadNode, bb.array(), false, false);
        if (node == null) {
            throw new IllegalStateException(
                    "Unable to create process node with path: " + SN_ROOT_NODE + "/" + myId + "_" + deadNode);
        }
        System.out.println("Published: " + node);
    }

    public Integer getSN(int nextNode, int deadNode) {
        byte[] nodeData = zooKeeperService.getNodeData(SN_ROOT_NODE + "/" + nextNode + "_" + deadNode, false);
        System.out.println("Searching: " + SN_ROOT_NODE + "/" + nextNode + "_" + deadNode);
        if(nodeData == null) {
            //List<String> children = zooKeeperService.getChildren(SN_ROOT_NODE, false);
            System.out.println("Search failed");
            return null;
        }
        System.out.println("Search success");
        zooKeeperService.deleteNode(SN_ROOT_NODE + "/" + nextNode + "_" + deadNode);
        ByteBuffer bb = ByteBuffer.wrap(nodeData);
        return bb.getInt();
    }



    private void membershipChanged() {
        final List<String> children = zooKeeperService.getChildren(LEADER_ELECTION_ROOT_NODE, true);
        List<Integer> l = children.stream().map(s -> Integer.parseInt(s.substring(s.lastIndexOf("_") + 1)))
                .sorted().collect(Collectors.toList());
        list.membershipChanged(myId, l);
    }

    public class ProcessNodeWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType().equals(Event.EventType.NodeChildrenChanged)) {
                if (event.getPath().equalsIgnoreCase(LEADER_ELECTION_ROOT_NODE)) {
                    membershipChanged();
                }
            }
        }
    }
}