package lab.minikafka;

import java.util.ArrayList;
import java.util.List;

/**
 * One in-memory append-only log.
 *
 * <p>This is the smallest Kafka-shaped data structure in the lab. There is no topic name yet and no
 * disk persistence yet; the only invariant is that records are appended at monotonically increasing
 * offsets and can be replayed from an offset.
 */
public final class PartitionLog {

  /*
   * The list index is deliberately the offset in this first stage. Later stages keep the same
   * offset contract even after the storage changes to files and segments.
   */
  private final List<Message> messages = new ArrayList<>();

  public synchronized long append(byte[] key, byte[] value) {
    // A new record's offset is the current end of the log.
    long offset = messages.size();
    messages.add(new Message(offset, key, value));
    return offset;
  }

  public synchronized FetchResult fetch(long offset, int maxMessages) {
    List<Message> batch = readFrom(offset, maxMessages);
    /*
     * nextOffset is a position, not a message id. It points at the next record the caller should
     * request after processing this batch.
     */
    return new FetchResult(batch, offset + batch.size());
  }

  public synchronized long endOffset() {
    return messages.size();
  }

  private List<Message> readFrom(long offset, int maxMessages) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be >= 0");
    }
    if (maxMessages <= 0) {
      throw new IllegalArgumentException("maxMessages must be > 0");
    }
    if (offset >= messages.size()) {
      return List.of();
    }

    // At this stage there are no retention gaps, so offset maps directly to the list index.
    int start = Math.toIntExact(offset);
    int end = Math.min(messages.size(), start + maxMessages);
    return List.copyOf(messages.subList(start, end));
  }
}
