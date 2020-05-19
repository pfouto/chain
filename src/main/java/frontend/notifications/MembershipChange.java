package frontend.notifications;

import babel.generic.ProtoNotification;

import java.net.InetAddress;
import java.util.List;

public class MembershipChange extends ProtoNotification {

    public final static short NOTIFICATION_ID = 102;

    private final List<InetAddress> orderedMembers;
    private final InetAddress readsTo;
    private final InetAddress writesTo;
    private final InetAddress responder;

    public MembershipChange(List<InetAddress> orderedMembers, InetAddress readsTo,
                            InetAddress writesTo, InetAddress responder) {
        super(NOTIFICATION_ID);
        this.orderedMembers = orderedMembers;
        this.readsTo = readsTo;
        this.writesTo = writesTo;
        this.responder = responder;
    }

    public InetAddress getReadsTo() {
        return readsTo;
    }

    public InetAddress getWritesTo() {
        return writesTo;
    }

    public InetAddress getResponder() { return responder; }

    public List<InetAddress> getOrderedMembers() {
        return orderedMembers;
    }
}
