package chainpaxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class JoinTimer extends ProtoTimer {

    public static final short TIMER_ID = 201;

    public static final JoinTimer instance = new JoinTimer();

    private JoinTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
