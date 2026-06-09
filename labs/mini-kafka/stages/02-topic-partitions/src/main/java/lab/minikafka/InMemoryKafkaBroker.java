package lab.minikafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node broker for the first mini-Kafka milestone.
 *
 * <p>This class keeps the minimum concepts needed to explain Kafka's core model: topic-partitions
 * are independent logs, producers append to a partition, and consumers fetch from an offset.
 */
public final class InMemoryKafkaBroker {

  /*
   * Kafka's real metadata model is much richer. This stage uses the presence of a TopicPartition
   * key as the entire topic catalog: if the key exists, that partition exists.
   */
  private final Map<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();

  public void createTopic(String topic, int partitions) {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be > 0");
    }
    for (int partition = 0; partition < partitions; partition++) {
      /*
       * Creating a topic means creating one independent log per partition. Each partition owns its
       * own offset sequence, which is why partition 0 and partition 1 can both have offset 0.
       */
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

  public long endOffset(String topic, int partition) {
    return logFor(topic, partition).endOffset();
  }

  private PartitionLog logFor(String topic, int partition) {
    // The broker API accepts primitives, but the map key is the actual partition identity.
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    PartitionLog log = logs.get(topicPartition);
    if (log == null) {
      throw new IllegalArgumentException("unknown topic partition: " + topicPartition);
    }
    return log;
  }
}
