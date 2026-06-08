package lab.minikafka.broker;

import lab.minikafka.model.TopicPartition;
import lab.minikafka.storage.InMemoryPartitionLog;
import lab.minikafka.storage.PartitionLogStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-node broker for the first mini-Kafka milestone.
 *
 * <p>This class keeps the minimum concepts needed to explain Kafka's core model: topic-partitions
 * are independent logs, producers append to a partition, consumers fetch from an offset, and
 * consumer groups track progress separately from the log itself.
 */
public final class InMemoryKafkaBroker extends AbstractSingleNodeKafkaBroker {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryKafkaBroker.class);

  @Override
  public void createTopic(String topic, int partitions) {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be > 0");
    }
    LOG.info("Creating topic '{}' with {} partitions", topic, partitions);
    for (int partition = 0; partition < partitions; partition++) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      ensureTopicPartitionAvailable(topicPartition);
      PartitionLogStore previous = logs().putIfAbsent(topicPartition, new InMemoryPartitionLog());
      if (previous != null) {
        throw new IllegalArgumentException("topic partition already exists: " + topicPartition);
      }
      LOG.debug("Created partition log for {}", topicPartition);
    }
  }
}
