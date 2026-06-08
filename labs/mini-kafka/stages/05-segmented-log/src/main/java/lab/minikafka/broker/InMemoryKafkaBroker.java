package lab.minikafka.broker;

import lab.minikafka.model.TopicPartition;
import lab.minikafka.storage.InMemoryPartitionLog;
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
    validatePartitionCount(partitions);
    LOG.info("Creating topic '{}' with {} partitions", topic, partitions);
    for (int partition = 0; partition < partitions; partition++) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      registerLog(topicPartition, new InMemoryPartitionLog());
      LOG.debug("Created partition log for {}", topicPartition);
    }
  }
}
