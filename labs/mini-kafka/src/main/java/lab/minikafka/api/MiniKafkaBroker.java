package lab.minikafka.api;

/**
 * Small broker interface for the first mini-Kafka milestones.
 *
 * <p>This keeps tests and future implementations decoupled from one concrete broker class. The
 * current implementation is in-memory, but the next milestone can introduce a disk-backed broker
 * behind the same contract.
 */
public interface MiniKafkaBroker {

    void createTopic(String topic, int partitions);

    long append(String topic, int partition, byte[] key, byte[] value);

    FetchResult fetch(String topic, int partition, long offset, int maxMessages);

    void commitOffset(String groupId, String topic, int partition, long offset);

    long committedOffset(String groupId, String topic, int partition);

    long endOffset(String topic, int partition);
}
