package chainpaxos.values;

import common.WriteBatch;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Objects;

public class AppOpBatch extends PaxosValue {

    private final WriteBatch batch;

    public AppOpBatch(WriteBatch batch) {
        super(Type.APP_BATCH);
        this.batch = batch;
    }

    public WriteBatch getBatch() {
        return batch;
    }

    @Override
    public String toString() {
        return "AppOpBatch{" +
                "batch=" + batch +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AppOpBatch)) return false;
        AppOpBatch that = (AppOpBatch) o;
        return batch.equals(that.batch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batch);
    }

    static ValueSerializer serializer = new ValueSerializer<AppOpBatch>() {
        @Override
        public void serialize(AppOpBatch appOpBatch, ByteBuf out) throws IOException {
            WriteBatch.serializer.serialize(appOpBatch.batch, out);
        }

        @Override
        public AppOpBatch deserialize(ByteBuf in) throws IOException {
            WriteBatch batch = WriteBatch.serializer.deserialize(in);
            return new AppOpBatch(batch);
        }
    };
}
