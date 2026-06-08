package lab.minikafka.api;

/**
 * Small broker interface for the first mini-Kafka milestones.
 *
 * <p>This keeps tests and future implementations decoupled from one concrete broker class. The
 * current implementations share the same single-node semantics: topic partitions are independent
 * logs, append returns the assigned partition offset, fetch reads from an offset, and consumer
 * group progress is tracked separately from message storage.
 */
public interface MiniKafkaBroker {

  /**
   * Creates a topic with a fixed number of partitions.
   *
   * <p>This teaching API intentionally has no topic deletion, partition expansion, or metadata
   * controller. A topic is available after this call registers one log per partition.
   */
  void createTopic(String topic, int partitions);

  /**
   * Appends one record to an existing topic partition.
   *
   * @return the offset assigned to the appended record within that partition
   */
  long append(String topic, int partition, byte[] key, byte[] value);

  /**
   * Reads up to {@code maxMessages} records from a topic partition starting at {@code offset}.
   *
   * <p>The returned {@link FetchResult#nextOffset()} can be used as the next fetch position or as a
   * committed offset after processing the returned records.
   */
  FetchResult fetch(String topic, int partition, long offset, int maxMessages);

  /**
   * Stores a consumer group's progress for one topic partition.
   *
   * <p>Committed offsets are intentionally modeled as consumer progress, not as message deletion.
   * Records remain readable until a future retention milestone removes them.
   */
  void commitOffset(String groupId, String topic, int partition, long offset);

  /**
   * Returns a consumer group's committed offset for one topic partition.
   *
   * <p>A group that has never committed an offset starts at {@code 0}, matching the beginning of
   * the partition log in this phase.
   */
  long committedOffset(String groupId, String topic, int partition);

  /**
   * Returns the next offset that would be assigned in a topic partition.
   *
   * <p>This is the partition high-water mark for the single-node lab. Real Kafka separates log end,
   * high watermark, and last stable offset once replication and transactions enter the picture.
   */
  long endOffset(String topic, int partition);
}
