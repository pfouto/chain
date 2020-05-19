package chainreplication.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Membership {

    private static final Logger logger = LogManager.getLogger(Membership.class);

    private List<Integer> members;
    private Map<Integer, Integer> indexMap;
    private int myId;
    private boolean head;
    private boolean tail;

    public Membership(int myId, List<Integer> initial) {
        this.myId = myId;
        members = new ArrayList<>(initial);
        indexMap = new HashMap<>();
        head = (indexOf(myId) == 0);
        tail = (indexOf(myId) == members.size() - 1);
        //logger.info("New " + this + " head: " + head + " tail: " + tail);
    }


    public List<Integer> getMembers() {
        return members;
    }

    public int headId() {
        return members.get(0);
    }

    public int tailId() {
        return members.get(members.size() - 1);
    }

    public boolean isHead() {
        return head;
    }

    public boolean isTail() {
        return tail;
    }

    public int getMyId() {
        return myId;
    }

    public int nextNode() {
        return members.get(indexOf(myId) + 1);
    }

    public int prevNode() {
        return members.get(indexOf(myId) - 1);
    }

    public int indexOf(Integer host) {
        return indexMap.computeIfAbsent(host, members::indexOf);
    }

    public boolean contains(Integer host) {
        return indexOf(host) >= 0;
    }

    @Override
    public String toString() {
        return "{" + "members=" + members + '}';
    }

    public int size() {
        return members.size();
    }
}
