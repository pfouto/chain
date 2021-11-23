package common.values;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Objects;

public class MembershipOp extends PaxosValue {

    public enum OpType {
        ADD(0),
        REMOVE(1);

        private final int value;

        OpType(int value) {
            this.value = value;
        }

        static OpType fromValue(int value) {
            for (OpType type : OpType.values()) {
                if (type.value == value) {
                    return type;
                }
            }

            return null;
        }

        int value() {
            return value;
        }
    }

    public final OpType opType;
    public final Host affectedHost;
    public final int position; //to-remove hack

    private MembershipOp(OpType opType, Host affectedHost, int position) {
        super(Type.MEMBERSHIP);
        this.opType = opType;
        this.affectedHost = affectedHost;
        this.position = position;
    }

    public static MembershipOp RemoveOp(Host affectedHost){
        return new MembershipOp(OpType.REMOVE, affectedHost, -1);
    }

    public static MembershipOp AddOp(Host affectedHost, int position){
        return new MembershipOp(OpType.ADD, affectedHost, position);
    }

    public static MembershipOp AddOp(Host affectedHost){
        return new MembershipOp(OpType.ADD, affectedHost, -1);
    }

    @Override
    public String toString() {
        return opType + " " + affectedHost;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MembershipOp)) return false;
        MembershipOp that = (MembershipOp) o;
        return opType == that.opType &&
                affectedHost.equals(that.affectedHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, affectedHost);
    }

    public static ValueSerializer serializer = new ValueSerializer<MembershipOp>() {
        @Override
        public void serialize(MembershipOp membershipOp, ByteBuf out) throws IOException {
            out.writeInt(membershipOp.opType.value);
            Host.serializer.serialize(membershipOp.affectedHost, out);
            out.writeInt(membershipOp.position);
        }

        @Override
        public MembershipOp deserialize(ByteBuf in) throws IOException {
            OpType t = OpType.fromValue(in.readInt());
            Host affectedHost = Host.serializer.deserialize(in);
            int position = in.readInt();
            return new MembershipOp(t, affectedHost, position);
        }
    };

}
