package distinguishedpaxos.timers;

import babel.generic.ProtoTimer;

public class LeaderTimer extends ProtoTimer {

    public static final short TIMER_ID = 402;

    public static final LeaderTimer instance = new LeaderTimer();

    private LeaderTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
