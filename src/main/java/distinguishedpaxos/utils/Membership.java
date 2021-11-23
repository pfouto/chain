package distinguishedpaxos.utils;

import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);

    private LinkedHashSet<Host> members;

    public Membership(Collection<Host> initial) {
        members = new LinkedHashSet<>(initial);
        logger.debug("New " + this);
    }

    public boolean contains(Host host) {
        return members.contains(host);
    }

    public int size() {
        return members.size();
    }

    public Set<Host> getMembers() {
        return Collections.unmodifiableSet(members);
    }

    @Override
    public String toString() {
        return "{" + "members=" + members + '}';
    }

}
