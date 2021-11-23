package frontend.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class InfoTimer extends ProtoTimer {

    public static final short TIMER_ID = 104;

    public InfoTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
