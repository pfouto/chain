package epaxos.utils;

import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);

    private List<Host> members;
    private List<Host> preferredMembers;
    private Host self;

    private int fastPathSize;

    public Membership(Host self, Collection<Host> initial, int MAX_CONCURRENT_FAILS) {
        this.fastPathSize = MAX_CONCURRENT_FAILS + ((MAX_CONCURRENT_FAILS +1) / 2);
        this.members = new LinkedList<>(initial);
        this.preferredMembers = new LinkedList<>();
        this.self = self;
        chooseNewPreferred();
        logger.debug("New " + this);

    }

    private void chooseNewPreferred(){
        preferredMembers.clear();
        int myIndex = members.indexOf(self);
        int nPreferred = getFastPathSizeExcludingSelf();

        for (int i = myIndex + 1 ; i <= myIndex + nPreferred; i++){
            assert !members.get(i%members.size()).equals(self);
            preferredMembers.add(members.get(i%members.size()));
        }

        logger.info("New preferred members: " + preferredMembers);
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

    public List<Host> shallowCopy() {
        return new ArrayList<>(members);
    }

    @Override
    public String toString() {
        return "{" + "members=" + members + '}';
    }

    public Iterator<Host> getMajorityQuorumReplicasExcludingSelf() {
        return preferredMembers.iterator();
    }

    public Iterator<Host> getFastPathReplicasExcludingSelf() {
        return preferredMembers.iterator();
    }

    public int getFastPathSizeExcludingSelf() {
        return fastPathSize-1;
    }

    public int getMajorityQuorumSizeExcludingSelf() {
        return members.size()/2;
    }

    public Iterator<Host> allExceptMe() {
        return members.stream().filter(h -> !h.equals(self)).collect(
                Collectors.toCollection(ArrayList::new)).iterator();
    }
}
