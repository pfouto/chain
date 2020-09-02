package chainpaxos.utils;


import common.values.PaxosValue;
import frontend.ops.ReadOp;
import network.data.Host;

import java.util.*;

public class InstanceState {

    public final int iN;
    public SeqN highestAccept;
    public PaxosValue acceptedValue;
    public short counter;

    private boolean decided;

    public Map<SeqN, Set<Host>> prepareResponses;

    private Queue<ReadOp> attachedReads;

    public InstanceState(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue = null;
        this.counter = 0;
        this.decided = false;
        this.prepareResponses = new HashMap<>();
        this.attachedReads = new LinkedList<>();
    }

    @Override
    public String toString() {
        return "InstanceState{" +
                "iN=" + iN +
                ", highestAccept=" + highestAccept +
                ", acceptedValue=" + acceptedValue +
                ", counter=" + counter +
                ", decided=" + decided +
                ", prepareResponses=" + prepareResponses +
                '}';
    }

    public void attachRead(ReadOp op) {
        if(decided) throw new IllegalStateException();
        attachedReads.add(op);
    }

    public Queue<ReadOp> getAttachedReads() {
        return attachedReads;
    }

    //If it is already decided by some node, or received from prepareOk
    public void forceAccept(SeqN sN, PaxosValue value) {
        assert sN.getCounter() > -1;
        assert value != null;
        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        assert !isDecided() || acceptedValue.equals(value);
        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);

        this.highestAccept = sN.greaterThan(this.highestAccept) ? sN : this.highestAccept;
        this.acceptedValue = value;
        this.counter = -1;
    }

    public void accept(SeqN sN, PaxosValue value, short counter) {
        assert sN.getCounter() > -1;
        assert value != null;
        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        assert !isDecided() || acceptedValue.equals(value);
        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);

        this.highestAccept = sN;
        this.acceptedValue = value;
        this.counter = counter;
    }

    public boolean isDecided() {
        return decided;
    }

    public void markDecided() {
        assert acceptedValue != null && highestAccept != null;
        assert !decided;
        decided = true;
    }
}
