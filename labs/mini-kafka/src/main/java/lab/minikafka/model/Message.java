package lab.minikafka.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Minimal record stored inside a partition log.
 *
 * <p>The most important field here is the offset. It represents the record's position in one
 * partition and is the basis for ordered reads and recovery.
 */
public final class Message {

    private final long offset;
    private final byte[] key;
    private final byte[] value;

    public Message(long offset, byte[] key, byte[] value) {
        this.offset = offset;
        this.key = key == null ? null : Arrays.copyOf(key, key.length);
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public long offset() {
        return offset;
    }

    public byte[] key() {
        return key == null ? null : Arrays.copyOf(key, key.length);
    }

    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Message)) {
            return false;
        }
        Message that = (Message) other;
        return offset == that.offset
            && Arrays.equals(key, that.key)
            && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(offset);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
