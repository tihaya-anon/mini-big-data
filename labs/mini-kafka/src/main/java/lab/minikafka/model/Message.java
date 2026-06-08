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
    key = copy(key);
    value = copy(value);
  }

  @Override
  public byte[] key() {
    return copy(key);
  }

  @Override
  public byte[] value() {
    return copy(value);
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || other instanceof Message that
            && offset == that.offset
            && Arrays.equals(key, that.key)
            && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(offset);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  private static byte[] copy(byte[] bytes) {
    return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
  }
}
