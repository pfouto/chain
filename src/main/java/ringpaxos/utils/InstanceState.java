package ringpaxos.utils;

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

    private boolean canDecide;

    public Map<SeqN, Set<Host>> prepareResponses;

    //public int nDecisionReqs = 0;
    //public int nAcceptReqs = 0;

    long acceptReqTS;
    long decisionReqTS;
    long acceptSentTime;

    public InstanceState(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue = null;
        this.decided = false;
        this.prepareResponses = new HashMap<>();
        acceptReqTS = 0;
        decisionReqTS = 0;
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

    public long getAcceptSentTime() {
        return acceptSentTime;
    }

    public void setAcceptSentTime(long acceptSentTime) {
        this.acceptSentTime = acceptSentTime;
    }

    public long getAcceptReqTS() {
        return acceptReqTS;
    }

    public void setAcceptReqTS(long acceptReqTS) {
        this.acceptReqTS = acceptReqTS;
    }

    public void setDecisionReqTS(long decisionReqTS) {
        this.decisionReqTS = decisionReqTS;
    }

    public long getDecisionReqTS() {
        return decisionReqTS;
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

    public boolean canDecide() {
        return canDecide;
    }

    public void markCanDecide(SeqN sN) {
        assert highestAccept == null || sN.greaterOrEqualsThan(highestAccept);
        highestAccept = sN;
        canDecide = true;
    }

    public void markDecided() {
        assert acceptedValue != null && highestAccept != null;
        assert !decided;
        decided = true;
    }
}
