package lab.minikafka.broker;

import java.io.IOException;
import java.nio.file.Path;
import lab.minikafka.model.TopicPartition;
import lab.minikafka.storage.FilePartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disk-backed broker for the first persistence milestone.
 */
public final class FileBackedKafkaBroker extends AbstractSingleNodeKafkaBroker {

    private static final Logger LOG = LoggerFactory.getLogger(FileBackedKafkaBroker.class);

    private final Path dataDirectory;

    public FileBackedKafkaBroker(Path dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    @Override
    public void createTopic(String topic, int partitions) {
        validatePartitionCount(partitions);
        LOG.info("Creating disk-backed topic '{}' with {} partitions", topic, partitions);
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            createOrRecoverLog(topicPartition);
            LOG.debug("Created file-backed partition log for {}", topicPartition);
        }
    }

    private void createOrRecoverLog(TopicPartition topicPartition) {
        try {
            registerLog(topicPartition, new FilePartitionLog(pathFor(topicPartition)));
        } catch (IOException exception) {
            throw new IllegalStateException("failed to create log for " + topicPartition, exception);
        }
    }

    private Path pathFor(TopicPartition topicPartition) {
        return dataDirectory
            .resolve(topicPartition.topic())
            .resolve("partition-" + topicPartition.partition() + ".log");
    }
}
