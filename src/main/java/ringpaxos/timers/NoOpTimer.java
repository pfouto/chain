package ringpaxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class NoOpTimer extends ProtoTimer {

    public static final short TIMER_ID = 605;

    public static final NoOpTimer instance = new NoOpTimer();

    private NoOpTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
