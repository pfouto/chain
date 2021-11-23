package common.values;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public abstract class PaxosValue {

    interface ValueSerializer<T extends PaxosValue> extends ISerializer<T> {
    }

    //Enum adapted from Apache Cassandra
    public enum Type {
        MEMBERSHIP(0, MembershipOp.serializer),
        APP_BATCH(1, AppOpBatch.serializer),
        NO_OP(2, NoOpValue.serializer);

        public final int opcode;
        private final ValueSerializer<PaxosValue> serializer;

        private static final Type[] opcodeIdx;

        static {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values()) {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        Type(int opcode, ValueSerializer<PaxosValue> serializer) {
            this.opcode = opcode;
            this.serializer = serializer;
        }

        public static Type fromOpcode(int opcode) {
            if (opcode >= opcodeIdx.length || opcode < 0)
                throw new AssertionError(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new AssertionError(String.format("Unknown opcode %d", opcode));
            return t;
        }
    }

    public final Type type;

    PaxosValue(Type type) {
        this.type = type;
    }

    @Override
    public abstract boolean equals(Object other);

    public static final ISerializer<PaxosValue> serializer = new ISerializer<>() {
        public void serialize(PaxosValue value, ByteBuf out) throws IOException {
            out.writeInt(value.type.opcode);
            value.type.serializer.serialize(value, out);
        }

        public PaxosValue deserialize(ByteBuf in) throws IOException {
            Type type = Type.fromOpcode(in.readInt());
            return type.serializer.deserialize(in);
        }
    };
}
