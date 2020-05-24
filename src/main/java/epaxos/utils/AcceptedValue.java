package epaxos.utils;

import common.values.PaxosValue;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.UnknownHostException;

public class AcceptedValue {
    public final int instance;
    public final int sequenceNumber;
    public final PaxosValue value;

    public AcceptedValue(int instance, int sequenceNumber, PaxosValue value) {
        this.instance = instance;
        this.sequenceNumber = sequenceNumber;
        this.value = value;
    }

    @Override
    public String toString() {
        return "AV{" +
                "i=" + instance +
                ", sn=" + sequenceNumber +
                ", v=" + value +
                '}';
    }

    public void serialize(ByteBuf out) throws IOException {
        out.writeInt(instance);
        out.writeInt(sequenceNumber);
        PaxosValue.serializer.serialize(value, out);
    }

    public static AcceptedValue deserialize(ByteBuf in) throws IOException {
        int acceptedInstance = in.readInt();
        int acceptedValueSequenceNumber = in.readInt();
        PaxosValue acceptedValue = PaxosValue.serializer.deserialize(in);
        return new AcceptedValue(acceptedInstance, acceptedValueSequenceNumber, acceptedValue);
    }
}
