package uringpaxos.utils;

import common.values.PaxosValue;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InstanceState {

    public final int iN;
    public SeqN highestAccept;
    public PaxosValue acceptedValue;

    private boolean decided;

    public Map<SeqN, Set<Host>> prepareResponses;


    public InstanceState(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue = null;
        this.decided = false;
        this.prepareResponses = new HashMap<>();
    }

    //If it is already decided by some node, or received from prepareok
    public void forceAccept(SeqN sN, PaxosValue value) {
        assert sN.getCounter() > -1;
        assert value != null;

        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        assert !isDecided() || acceptedValue.equals(value);
        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue.equals(value);

        this.highestAccept = (this.highestAccept == null || sN.greaterThan(
                this.highestAccept)) ? sN : this.highestAccept;
        this.acceptedValue = value;
    }

    public void accept(SeqN sN, PaxosValue value) {
        assert sN.getCounter() > -1;
        assert value != null;
        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        assert !isDecided() || acceptedValue.equals(value);
        assert highestAccept == null || sN.greaterThan(highestAccept) || acceptedValue == null ||
                acceptedValue.equals(value);

        highestAccept = sN;
        acceptedValue = value;
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
