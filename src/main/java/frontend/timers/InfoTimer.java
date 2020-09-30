package frontend.timers;

import babel.generic.ProtoTimer;

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
