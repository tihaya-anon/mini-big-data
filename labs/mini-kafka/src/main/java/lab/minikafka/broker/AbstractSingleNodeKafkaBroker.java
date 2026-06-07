package lab.minikafka.broker;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lab.minikafka.api.FetchResult;
import lab.minikafka.api.MiniKafkaBroker;
import lab.minikafka.model.Message;
import lab.minikafka.model.TopicPartition;
import lab.minikafka.storage.PartitionLogStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared broker flow for single-node mini-Kafka implementations.
 */
abstract class AbstractSingleNodeKafkaBroker implements MiniKafkaBroker {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSingleNodeKafkaBroker.class);

    private final Map<TopicPartition, PartitionLogStore> logs = new ConcurrentHashMap<>();
    private final Map<String, Map<TopicPartition, Long>> groupOffsets = new ConcurrentHashMap<>();

    protected final void validatePartitionCount(int partitions) {
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be > 0");
        }
    }

    @Override
    public final long append(String topic, int partition, byte[] key, byte[] value) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        long offset = appendToLog(topicPartition, key, value);
        LOG.info("Appended record to {} at offset {}", topicPartition, offset);
        return offset;
    }

    @Override
    public final FetchResult fetch(String topic, int partition, long offset, int maxMessages) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<Message> messages = readFromLog(topicPartition, offset, maxMessages);
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

    @Override
    public final void commitOffset(String groupId, String topic, int partition, long offset) {
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
    public final long committedOffset(String groupId, String topic, int partition) {
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
    public final long endOffset(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        long endOffset = logFor(topicPartition).endOffset();
        LOG.debug("End offset for {} is {}", topicPartition, endOffset);
        return endOffset;
    }

    protected final PartitionLogStore logFor(TopicPartition topicPartition) {
        PartitionLogStore log = logs.get(topicPartition);
        if (log == null) {
            throw new IllegalArgumentException("unknown topic partition: " + topicPartition);
        }
        return log;
    }

    protected final Map<TopicPartition, PartitionLogStore> logs() {
        return logs;
    }

    protected final void ensureTopicPartitionAvailable(TopicPartition topicPartition) {
        if (logs.containsKey(topicPartition)) {
            throw new IllegalArgumentException("topic partition already exists in broker: " + topicPartition);
        }
    }

    protected final void registerLog(TopicPartition topicPartition, PartitionLogStore logStore) {
        ensureTopicPartitionAvailable(topicPartition);
        PartitionLogStore previous = logs.putIfAbsent(topicPartition, logStore);
        if (previous != null) {
            throw new IllegalArgumentException("topic partition already exists in broker: " + topicPartition);
        }
    }

    protected final void ensureExists(TopicPartition topicPartition) {
        if (!logs.containsKey(topicPartition)) {
            throw new IllegalArgumentException("unknown topic partition: " + topicPartition);
        }
    }

    private long appendToLog(TopicPartition topicPartition, byte[] key, byte[] value) {
        try {
            return logFor(topicPartition).append(key, value);
        } catch (IOException exception) {
            throw new IllegalStateException("failed to append to " + topicPartition, exception);
        }
    }

    private List<Message> readFromLog(TopicPartition topicPartition, long offset, int maxMessages) {
        try {
            return logFor(topicPartition).readFrom(offset, maxMessages);
        } catch (IOException exception) {
            throw new IllegalStateException("failed to read from " + topicPartition, exception);
        }
    }
}
