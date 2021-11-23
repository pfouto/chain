package frontend.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class StateTimer extends ProtoTimer {

    public static final short TIMER_ID = 102;

    public StateTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
