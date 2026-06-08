package lab.minikafka;

import java.util.Arrays;
import java.util.Objects;

public final class Message {

  private final long offset;
  private final byte[] key;
  private final byte[] value;

  public Message(long offset, byte[] key, byte[] value) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be >= 0");
    }
    this.offset = offset;
    this.key = copy(key);
    this.value = copy(value);
  }

  public long offset() {
    return offset;
  }

  public byte[] key() {
    return copy(key);
  }

  public byte[] value() {
    return copy(value);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Message that)) {
      return false;
    }
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

  private static byte[] copy(byte[] bytes) {
    return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
  }
}
