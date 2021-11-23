package chainreplication.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.List;

public class MembershipChangeEvt extends ProtoRequest {

    public static final short REQUEST_ID = 301;

    public int myId;
    public List<Integer> l;

    public MembershipChangeEvt(int myId, List<Integer> l) {
        super(REQUEST_ID);
        this.myId = myId;
        this.l = l;
    }

    @Override
    public String toString() {
        return "MembershipChangeEvt{" +
                "myId=" + myId +
                ", l=" + l +
                '}';
    }
}
