package epaxos.utils;


import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Objects;

public class GraphNode {
    public final Host replica;
    public final int iN;
    public boolean onStack;

    public int index;
    public int lowLink;

    public GraphNode(Host replica, int iN) {
        if(replica == null) throw new AssertionError("replica null");
        this.replica = replica;
        this.iN = iN;
        onStack = false;
        index = -1;
        lowLink = -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GraphNode)) return false;
        GraphNode graphNode = (GraphNode) o;
        return iN == graphNode.iN &&
                Objects.equals(replica, graphNode.replica);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replica, iN);
    }
}
