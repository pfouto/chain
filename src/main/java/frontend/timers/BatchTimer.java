package frontend.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class BatchTimer extends ProtoTimer {

    public static final short TIMER_ID = 101;

    public BatchTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
