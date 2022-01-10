package distinguishedpaxos.utils;

import common.values.PaxosValue;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InstanceState {

    public final int iN;
    public SeqN highestAccept;
    public PaxosValue acceptedValue;
    public Map<SeqN, Set<Host>> prepareResponses;
    public int nodesDecided;
    private boolean decided;
    private Set<Host> accepteds;
    private boolean peerDecided;

    public InstanceState(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue = null;
        this.decided = false;
        this.prepareResponses = new HashMap<>();
        this.accepteds = new HashSet<>();
        this.peerDecided = false;
        this.nodesDecided = 0;
    }

    //If it is already decided by some node, or received from prepareok
    public void forceAccept(SeqN sN, PaxosValue value) {
        assert sN.getCounter() > -1;
        assert value != null;

        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        assert !isDecided() || acceptedValue.equals(value);
        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);

        this.highestAccept = (this.highestAccept == null || sN.greaterThan(this.highestAccept)) ? sN : this.highestAccept;
        this.acceptedValue = value;
    }


    public void accept(SeqN sN, PaxosValue value) {

        if (highestAccept == null || sN.greaterThan(highestAccept)) {
            accepteds.clear();
        }
        highestAccept = sN;
        if (value != null)
            acceptedValue = value;
    }

    public int registerAccepted(SeqN sN, Host sender) {
        accept(sN, null);
        accepteds.add(sender);
        return accepteds.size();
    }

    public void registerPeerDecision(SeqN sN, PaxosValue value) {
        accept(sN, value);
        peerDecided = true;
    }

    public int getAccepteds() {
        return accepteds.size();
    }

    public boolean isPeerDecided() {
        return peerDecided;
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
