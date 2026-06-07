package lab.minikafka.broker;

import lab.minikafka.api.FetchResult;
import lab.minikafka.api.MiniKafkaBroker;
import lab.minikafka.model.Message;
import lab.minikafka.model.TopicPartition;
import lab.minikafka.storage.PartitionLog;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-node broker for the first mini-Kafka milestone.
 *
 * <p>This class keeps the minimum concepts needed to explain Kafka's core model:
 * topic-partitions are independent logs, producers append to a partition, consumers fetch from
 * an offset, and consumer groups track progress separately from the log itself.
 */
public final class InMemoryKafkaBroker implements MiniKafkaBroker {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryKafkaBroker.class);

    private final Map<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, Long>> groupOffsets = new ConcurrentHashMap<>();

    @Override
    public void createTopic(String topic, int partitions) {
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be > 0");
        }
        LOG.info("Creating topic '{}' with {} partitions", topic, partitions);
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            PartitionLog previous = logs.putIfAbsent(topicPartition, new PartitionLog());
            if (previous != null) {
                throw new IllegalArgumentException("topic partition already exists: " + topicPartition);
            }
            LOG.debug("Created partition log for {}", topicPartition);
        }
    }

    @Override
    public long append(String topic, int partition, byte[] key, byte[] value) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        long offset = logFor(topicPartition).append(key, value);
        LOG.info("Appended record to {} at offset {}", topicPartition, offset);
        return offset;
    }

    /**
     * Kafka consumers do not ask for "the next message object"; they ask for records starting at a
     * known offset. That is why fetch is modeled as "offset + batch size".
     */
    @Override
    public FetchResult fetch(String topic, int partition, long offset, int maxMessages) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        PartitionLog log = logFor(topicPartition);
        List<Message> messages = log.readFrom(offset, maxMessages);
        FetchResult result = new FetchResult(messages, offset + messages.size());
        LOG.info(
            "Fetched {} record(s) from {} starting at offset {}",
            messages.size(),
            topicPartition,
            offset
        );
        LOG.debug("Next offset for {} is {}", topicPartition, result.nextOffset());
        return result;
    }

    /**
     * A committed offset belongs to a consumer group, not to the log. This separation is central
     * to Kafka: the log stores records, while each group stores its own progress independently.
     */
    @Override
    public void commitOffset(String groupId, String topic, int partition, long offset) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        ensureExists(topicPartition);
        groupOffsets
            .computeIfAbsent(groupId, ignored -> new ConcurrentHashMap<>())
            .put(topicPartition, offset);
        LOG.info("Committed offset {} for group '{}' on {}", offset, groupId, topicPartition);
    }

    @Override
    public long committedOffset(String groupId, String topic, int partition) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        ensureExists(topicPartition);
        long offset = groupOffsets
            .getOrDefault(groupId, Map.of())
            .getOrDefault(topicPartition, 0L);
        LOG.debug("Read committed offset {} for group '{}' on {}", offset, groupId, topicPartition);
        return offset;
    }

    @Override
    public long endOffset(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        long endOffset = logFor(topicPartition).endOffset();
        LOG.debug("End offset for {} is {}", topicPartition, endOffset);
        return endOffset;
    }

    private PartitionLog logFor(TopicPartition topicPartition) {
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
