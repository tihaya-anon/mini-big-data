package lab.minikafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryKafkaBroker {

    private final Map<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();
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

    public FetchResult fetch(String topic, int partition, long offset, int maxMessages) {
        PartitionLog log = logFor(topic, partition);
        List<Message> messages = log.readFrom(offset, maxMessages);
        return new FetchResult(messages, offset + messages.size());
    }

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
    }

    public long committedOffset(String groupId, String topic, int partition) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        ensureExists(topicPartition);
        return groupOffsets
            .getOrDefault(groupId, Map.of())
            .getOrDefault(topicPartition, 0L);
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
