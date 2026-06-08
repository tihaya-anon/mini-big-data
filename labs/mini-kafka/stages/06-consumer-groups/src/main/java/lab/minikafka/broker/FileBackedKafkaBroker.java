package lab.minikafka.broker;

import java.io.IOException;
import java.nio.file.Path;
import lab.minikafka.model.TopicPartition;
import lab.minikafka.storage.FilePartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disk-backed broker for the first persistence milestone.
 *
 * <p>Each topic partition is stored under the configured data directory as a segmented {@link
 * FilePartitionLog}. Recreating the broker with the same directory and calling {@link
 * #createTopic(String, int)} reopens those segment files and recovers the partition end offset.
 *
 * <p>Only records are durable in this milestone. Topic definitions are recreated by calling {@code
 * createTopic}, and consumer group offsets remain in memory in {@link
 * AbstractSingleNodeKafkaBroker}.
 */
public final class FileBackedKafkaBroker extends AbstractSingleNodeKafkaBroker {

  private static final Logger LOG = LoggerFactory.getLogger(FileBackedKafkaBroker.class);

  private final Path dataDirectory;

  public FileBackedKafkaBroker(Path dataDirectory) {
    // All topic directories and partition logs are rooted below this broker data directory.
    this.dataDirectory = dataDirectory;
  }

  @Override
  public void createTopic(String topic, int partitions) {
    validatePartitionCount(partitions);
    LOG.info("Creating disk-backed topic '{}' with {} partitions", topic, partitions);

    /*
     * The same createTopic call is used for first creation and for restart recovery. If files
     * already exist, FilePartitionLog opens them and reconstructs end offsets from segment files.
     */
    for (int partition = 0; partition < partitions; partition++) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      createOrRecoverLog(topicPartition);
      LOG.debug("Created file-backed partition log for {}", topicPartition);
    }
  }

  private void createOrRecoverLog(TopicPartition topicPartition) {
    try {
      // The broker registers the recovered log through the same metadata path as a new log.
      registerLog(topicPartition, new FilePartitionLog(pathFor(topicPartition)));
    } catch (IOException exception) {
      throw new IllegalStateException("failed to create log for " + topicPartition, exception);
    }
  }

  private Path pathFor(TopicPartition topicPartition) {
    // Use stable relative names so the directory layout is easy to inspect in tests.
    return dataDirectory
        .resolve(topicPartition.topic())
        .resolve("partition-" + topicPartition.partition() + ".log");
  }
}
