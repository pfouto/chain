package chainpaxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class StateTransferTimer extends ProtoTimer {

    public static final short TIMER_ID = 205;

    public static final StateTransferTimer instance = new StateTransferTimer();

    private StateTransferTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
