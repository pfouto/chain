package common.values;

import io.netty.buffer.ByteBuf;

public class NoOpValue extends PaxosValue {

    public NoOpValue() {
        super(Type.NO_OP);
    }

    @Override
    public String toString() {
        return type.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof NoOpValue;
    }

    static ValueSerializer serializer = new ValueSerializer<NoOpValue>() {
        @Override
        public void serialize(NoOpValue appOpBatch, ByteBuf out) {
        }

        @Override
        public NoOpValue deserialize(ByteBuf in) {
            return new NoOpValue();
        }

    };
}
