package epaxos.utils;

import common.values.PaxosValue;
import epaxos.messages.AcceptOkMsg;
import epaxos.messages.PreAcceptOkMsg;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashMap;
import java.util.Map;

public class Instance {

    private PaxosValue value;
    private int seqNumber;
    private Map<Host, Integer> deps;
    private boolean executed;
    private InstanceState state;

    private Map<Host, PreAcceptOkMsg> preAcceptOkMsgs;
    private Map<Host, AcceptOkMsg> acceptOkMsgs;

    private Ballot ballot;

    final private Host replica;
    final private int iN;

    public enum InstanceState {PLACEHOLDER, PREACCEPTED, ACCEPTED, COMMITTED}

    public Instance(Host replica, int iN, int epoch, Host leader){
        this(replica, iN, new Ballot(epoch, 0, leader));
    }

    public Instance(Host replica, int iN, Ballot initialBallot) {
        this.value = null;
        this.seqNumber = -1;
        this.deps = new HashMap<>();
        this.state = InstanceState.PLACEHOLDER;
        this.executed = false;
        this.ballot = initialBallot;
        this.replica = replica;
        this.iN = iN;
    }

    @Override
    public String toString() {
        return "Instance{" +
                "rep=" + replica +
                ", iN=" + iN +
                ", val=" + value +
                ", sN=" + seqNumber +
                ", deps=" + deps +
                //", executed=" + executed +
                //", state=" + state +
                //", preAcceptOkMsgs=" + preAcceptOkMsgs +
                //", acceptOkMsgs=" + acceptOkMsgs +
                //", ballot=" + ballot +
                '}';
    }

    public void preAccept(PaxosValue value, int seqNumber, Map<Host, Integer> deps) {
        assert state == InstanceState.PLACEHOLDER || seqNumber > this.seqNumber ||
                (seqNumber == this.seqNumber && deps.equals(this.deps) && value.equals(this.value));
        this.value = value;
        this.seqNumber = seqNumber;
        this.deps = deps;
        this.state = InstanceState.PREACCEPTED;
        this.preAcceptOkMsgs = new HashMap<>();
    }

    public void accept(PaxosValue value, int seqNumber, Map<Host, Integer> deps) {
        assert state == InstanceState.PLACEHOLDER || state == InstanceState.PREACCEPTED || seqNumber > this.seqNumber ||
                (seqNumber == this.seqNumber && deps.equals(this.deps) && value.equals(this.value));
        this.value = value;
        this.seqNumber = seqNumber;
        this.deps = deps;
        this.state = InstanceState.ACCEPTED;
        this.preAcceptOkMsgs = null;
        this.acceptOkMsgs = new HashMap<>();
    }

    public void commit(PaxosValue value, int seqNumber, Map<Host, Integer> deps) {
        assert state == InstanceState.PLACEHOLDER || state == InstanceState.PREACCEPTED || state == InstanceState.ACCEPTED ||
                (seqNumber == this.seqNumber && deps.equals(this.deps) && value.equals(this.value));
        this.value = value;
        this.seqNumber = seqNumber;
        this.deps = deps;
        this.state = InstanceState.COMMITTED;
        this.preAcceptOkMsgs = null;
        this.acceptOkMsgs = null;
    }

    public void markExecuted() {
        assert state == InstanceState.COMMITTED && !executed;
        executed = true;
    }

    public void registerPreAcceptOkMessage(Host from, PreAcceptOkMsg msg) {
        assert state == InstanceState.PREACCEPTED;
        preAcceptOkMsgs.put(from, msg);
    }

    public void registerAcceptOkMessage(Host from, AcceptOkMsg msg) {
        assert state == InstanceState.ACCEPTED;
        acceptOkMsgs.put(from, msg);
    }

    public Map<Host, PreAcceptOkMsg> getPreAcceptOkMsgs() {
        return preAcceptOkMsgs;
    }

    public Map<Host, AcceptOkMsg> getAcceptOkMsgs() {
        return acceptOkMsgs;
    }

    public boolean isExecuted() {
        return executed;
    }

    public int getSeqNumber() {
        return seqNumber;
    }

    public Map<Host, Integer> getDeps() {
        return deps;
    }

    public InstanceState getState() {
        return state;
    }

    public PaxosValue getValue() {
        return value;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public Host getReplica() {
        return replica;
    }

    public int getiN() {
        return iN;
    }
}
