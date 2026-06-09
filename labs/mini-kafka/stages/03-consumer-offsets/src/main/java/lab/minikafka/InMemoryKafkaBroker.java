package lab.minikafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node broker for the first mini-Kafka milestone.
 *
 * <p>This class keeps the minimum concepts needed to explain Kafka's core model: topic-partitions
 * are independent logs, producers append to a partition, consumers fetch from an offset, and
 * consumer groups track progress separately from the log itself.
 */
public final class InMemoryKafkaBroker {

  private final Map<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();

  /*
   * Offsets are grouped first by consumer group, then by topic partition. This lets two groups read
   * the same physical log at different speeds without changing the stored messages.
   */
  private final Map<String, Map<TopicPartition, Long>> groupOffsets = new ConcurrentHashMap<>();

  public void createTopic(String topic, int partitions) {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be > 0");
    }
    for (int partition = 0; partition < partitions; partition++) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      PartitionLog previous = logs.putIfAbsent(topicPartition, new PartitionLog());
      if (previous != null) {
        throw new IllegalArgumentException("topic partition already exists: " + topicPartition);
      }
    }
  }

  public long append(String topic, int partition, byte[] key, byte[] value) {
    return logFor(topic, partition).append(key, value);
  }

  /**
   * Kafka consumers do not ask for "the next message object"; they ask for records starting at a
   * known offset. That is why fetch is modeled as "offset + batch size".
   */
  public FetchResult fetch(String topic, int partition, long offset, int maxMessages) {
    PartitionLog log = logFor(topic, partition);
    List<Message> messages = log.readFrom(offset, maxMessages);
    return new FetchResult(messages, offset + messages.size());
  }

  /**
   * A committed offset belongs to a consumer group, not to the log. This separation is central to
   * Kafka: the log stores records, while each group stores its own progress independently.
   */
  public void commitOffset(String groupId, String topic, int partition, long offset) {
    if (groupId == null || groupId.isBlank()) {
      throw new IllegalArgumentException("groupId must not be blank");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be >= 0");
    }

    TopicPartition topicPartition = new TopicPartition(topic, partition);
    ensureExists(topicPartition);

    /*
     * The committed value is the next offset to read, not the last processed record's offset. After
     * processing records 0 and 1, the group commits 2.
     */
    groupOffsets
        .computeIfAbsent(groupId, ignored -> new ConcurrentHashMap<>())
        .put(topicPartition, offset);
  }

  public long committedOffset(String groupId, String topic, int partition) {
    if (groupId == null || groupId.isBlank()) {
      throw new IllegalArgumentException("groupId must not be blank");
    }

    TopicPartition topicPartition = new TopicPartition(topic, partition);
    ensureExists(topicPartition);

    // A group that has never committed starts from the beginning in this teaching model.
    return groupOffsets.getOrDefault(groupId, Map.of()).getOrDefault(topicPartition, 0L);
  }

  public long endOffset(String topic, int partition) {
    return logFor(topic, partition).endOffset();
  }

  private PartitionLog logFor(String topic, int partition) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    PartitionLog log = logs.get(topicPartition);
    if (log == null) {
      throw new IllegalArgumentException("unknown topic partition: " + topicPartition);
    }
    return log;
  }

  private void ensureExists(TopicPartition topicPartition) {
    if (!logs.containsKey(topicPartition)) {
      throw new IllegalArgumentException("unknown topic partition: " + topicPartition);
    }
  }
}
