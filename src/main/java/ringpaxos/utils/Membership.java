package ringpaxos.utils;

import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);

    private List<Host> members;
    private Map<Host, Integer> indexMap;

    public Membership(Collection<Host> initial) {
        indexMap = new HashMap<>();
        members = new ArrayList<>(initial);

        logger.debug("New " + this);
    }

    public boolean contains(Host host) {
        return members.contains(host);
    }

    public int size() {
        return members.size();
    }

    public List<Host> getMembers() {
        return Collections.unmodifiableList(members);
    }

    public int indexOf(Host host) {
        return indexMap.computeIfAbsent(host, members::indexOf);
    }

    @Override
    public String toString() {
        return "{" + "members=" + members + '}';
    }

    public Host atIndex(int i) {
        if (i < 0) i = i + members.size();
        return members.get(i);
    }
}
