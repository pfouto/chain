package chainreplication.timer;

import babel.generic.ProtoTimer;

public class JoinTimer extends ProtoTimer {

    public static final short TIMER_ID = 301;

    public static final JoinTimer instance = new JoinTimer();

    private JoinTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
