package chainpaxos.timers;

import babel.generic.ProtoTimer;
import network.data.Host;

public class ReconnectTimer extends ProtoTimer {
    public static final short TIMER_ID = 204;

    private final Host host;

    public ReconnectTimer(Host host) {
        super(TIMER_ID);
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}
