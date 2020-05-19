package chainreplication.zookeeper;

import java.util.List;

public interface IMembershipListener {
    void membershipChanged(int myId, List<Integer> l);
}
