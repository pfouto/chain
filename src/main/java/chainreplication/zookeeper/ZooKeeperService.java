package chainreplication.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

class ZooKeeperService {

    private final ZooKeeper zooKeeper;

    ZooKeeperService(final String url, final ProcessNode.ProcessNodeWatcher processNodeWatcher) throws IOException {
        System.out.println(url);
        zooKeeper = new ZooKeeper(url, 4000, processNodeWatcher);
    }

    String createNode(final String node, byte[] data, final boolean watch, final boolean ephemeral) {

        String createdNodePath;
        try {
            final Stat nodeStat = zooKeeper.exists(node, watch);
            if (nodeStat == null)
                createdNodePath = zooKeeper.create(node, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        (ephemeral ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
            else
                createdNodePath = node;
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return createdNodePath;
    }

    void deleteNode(final String node) {
        try {
            zooKeeper.delete(node, -1);
        } catch (InterruptedException | KeeperException e) {
            throw new IllegalStateException(e);
        }
    }

    boolean watchNode(final String node, final boolean watch) {
        boolean watched = false;
        try {
            final Stat nodeStat = zooKeeper.exists(node, watch);
            if (nodeStat != null)
                watched = true;
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return watched;
    }

    List<String> getChildren(final String node, final boolean watch) {
        List<String> childNodes;
        try {
            childNodes = zooKeeper.getChildren(node, watch);
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return childNodes;
    }

    byte[] getNodeData(final String node, final boolean watch) {
        byte[] data;
        try {
            data = zooKeeper.getData(node, watch, new Stat());
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return data;
    }
}