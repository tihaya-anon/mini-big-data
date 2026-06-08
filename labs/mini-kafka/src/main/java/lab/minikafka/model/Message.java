package lab.minikafka.model;

import java.util.Arrays;

/**
 * Minimal record stored inside a partition log.
 *
 * <p>The most important field here is the offset. It represents the record's position in one
 * partition and is the basis for ordered reads and recovery.
 */
public record Message(long offset, byte[] key, byte[] value) {

  public Message {
    /*
     * byte[] is mutable, so a plain record would otherwise expose internal state. Copying at
     * construction makes a Message behave like an immutable value object.
     */
    key = copy(key);
    value = copy(value);
  }

  @Override
  public byte[] key() {
    // Return a copy for the same reason: callers should not be able to mutate this record.
    return copy(key);
  }

  @Override
  public byte[] value() {
    // A null value is allowed; it represents an absent payload, not an empty byte array.
    return copy(value);
  }

  @Override
  public boolean equals(Object other) {
    // Records do not compare arrays by content automatically, so equality must handle them itself.
    return this == other
        || other instanceof Message that
            && offset == that.offset
            && Arrays.equals(key, that.key)
            && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    // Keep hashCode consistent with the array-content equality above.
    int result = Long.hashCode(offset);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  private static byte[] copy(byte[] bytes) {
    return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
  }
}
