package lab.minikafka.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
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
 *
 * <p>The concrete broker chooses how partition logs are created. This base class handles the common
 * API behavior around lookup, append/fetch delegation, and in-memory consumer group offsets.
 *
 * <p>Keeping group offsets here makes an important boundary explicit: even the file-backed broker
 * in this milestone only persists records. Persisting committed offsets would require another
 * internal log, similar in spirit to Kafka's {@code __consumer_offsets} topic.
 */
abstract class AbstractSingleNodeKafkaBroker implements MiniKafkaBroker {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSingleNodeKafkaBroker.class);

  /*
   * Topic metadata is intentionally tiny in this lab. Instead of a separate Topic object, the
   * broker treats the presence of a TopicPartition key in this map as "that partition exists".
   */
  private final Map<TopicPartition, PartitionLogStore> logs = new ConcurrentHashMap<>();

  /*
   * A committed offset means "the next offset this group should read". For example, committing 3
   * means records at offsets 0, 1, and 2 have been processed for that group.
   */
  private final Map<String, Map<TopicPartition, Long>> groupOffsets = new ConcurrentHashMap<>();

  /*
   * Group membership is scoped by (groupId, topic) because this first consumer implementation only
   * supports subscribing to one topic. The TreeSet keeps assignment deterministic for tests and for
   * readers stepping through the algorithm.
   */
  private final Map<GroupKey, NavigableSet<String>> groupMembers = new ConcurrentHashMap<>();

  protected final void validatePartitionCount(int partitions) {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be > 0");
    }
  }

  @Override
  public final long append(String topic, int partition, byte[] key, byte[] value) {
    // The public API uses topic + partition primitives; internally one value object names the log.
    TopicPartition topicPartition = new TopicPartition(topic, partition);

    // Storage implementations assign the actual offset because they own the partition's end state.
    long offset = appendToLog(topicPartition, key, value);
    LOG.info("Appended record to {} at offset {}", topicPartition, offset);
    return offset;
  }

  @Override
  public final FetchResult fetch(String topic, int partition, long offset, int maxMessages) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    List<Message> messages = readFromLog(topicPartition, offset, maxMessages);

    /*
     * With no retention gaps yet, nextOffset is simply "requested offset + number of returned
     * records". If retention is added later, this may need to account for deleted ranges.
     */
    FetchResult result = new FetchResult(messages, offset + messages.size());
    LOG.info(
        "Fetched {} record(s) from {} starting at offset {}",
        messages.size(),
        topicPartition,
        offset);
    LOG.debug("Next offset for {} is {}", topicPartition, result.nextOffset());
    return result;
  }

  @Override
  public final List<TopicPartition> partitionsFor(String topic) {
    validateTopic(topic);

    /*
     * There is no separate topic catalog in this milestone. We derive the partition list from the
     * registered logs and sort it so assignment has stable input order.
     */
    List<TopicPartition> partitions =
        logs.keySet().stream()
            .filter(topicPartition -> topicPartition.topic().equals(topic))
            .sorted(Comparator.comparingInt(TopicPartition::partition))
            .toList();
    if (partitions.isEmpty()) {
      throw new IllegalArgumentException("unknown topic: " + topic);
    }
    return partitions;
  }

  @Override
  public final synchronized List<TopicPartition> joinConsumerGroup(
      String groupId, String consumerId, String topic) {
    validateGroupMember(groupId, consumerId);

    // This also fails fast if the topic is unknown, before mutating group membership.
    List<TopicPartition> partitions = partitionsFor(topic);

    /*
     * Joining only records membership. Assignment is recomputed from the whole member set instead
     * of stored as another mutable map, which keeps the toy rebalance logic easy to inspect.
     */
    GroupKey groupKey = new GroupKey(groupId, topic);
    NavigableSet<String> members =
        groupMembers.computeIfAbsent(groupKey, ignored -> new TreeSet<>());
    members.add(consumerId);

    List<TopicPartition> assignment = assignmentFor(partitions, members, consumerId);
    LOG.info(
        "Consumer '{}' joined group '{}' for topic '{}' with {} assigned partition(s)",
        consumerId,
        groupId,
        topic,
        assignment.size());
    return assignment;
  }

  @Override
  public final synchronized List<TopicPartition> assignedPartitions(
      String groupId, String consumerId, String topic) {
    validateGroupMember(groupId, consumerId);

    /*
     * Consumers ask for assignment before every poll/commit. That makes membership changes visible
     * without implementing Kafka's heartbeat and rebalance protocol yet.
     */
    List<TopicPartition> partitions = partitionsFor(topic);
    NavigableSet<String> members = groupMembers.get(new GroupKey(groupId, topic));
    return assignmentFor(partitions, members, consumerId);
  }

  @Override
  public final synchronized void leaveConsumerGroup(
      String groupId, String consumerId, String topic) {
    validateGroupMember(groupId, consumerId);

    // Keep the same topic validation behavior as join/assignment.
    partitionsFor(topic);

    GroupKey groupKey = new GroupKey(groupId, topic);
    NavigableSet<String> members = groupMembers.get(groupKey);
    if (members == null) {
      return;
    }

    /*
     * No assignment table needs to be edited here. Once the member disappears, the next
     * assignedPartitions call naturally redistributes partitions across the remaining members.
     */
    members.remove(consumerId);
    if (members.isEmpty()) {
      groupMembers.remove(groupKey);
    }
    LOG.info("Consumer '{}' left group '{}' for topic '{}'", consumerId, groupId, topic);
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

    // Offset commits are separate from log storage. Committing does not delete or mutate records.
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

    /*
     * A group with no commit starts at the beginning. Real Kafka makes this configurable with
     * auto.offset.reset; this lab hard-codes "earliest" because it is easiest to reason about.
     */
    long offset = groupOffsets.getOrDefault(groupId, Map.of()).getOrDefault(topicPartition, 0L);
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
    // Centralize the unknown-partition error so append/fetch/endOffset fail consistently.
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
    // Topic creation is idempotent neither in Kafka nor in this simplified API.
    if (logs.containsKey(topicPartition)) {
      throw new IllegalArgumentException(
          "topic partition already exists in broker: " + topicPartition);
    }
  }

  protected final void registerLog(TopicPartition topicPartition, PartitionLogStore logStore) {
    ensureTopicPartitionAvailable(topicPartition);

    /*
     * putIfAbsent keeps registration safe even if a future caller creates partitions concurrently.
     * The explicit pre-check above gives the common path a clear error message.
     */
    PartitionLogStore previous = logs.putIfAbsent(topicPartition, logStore);
    if (previous != null) {
      throw new IllegalArgumentException(
          "topic partition already exists in broker: " + topicPartition);
    }
  }

  protected final void ensureExists(TopicPartition topicPartition) {
    if (!logs.containsKey(topicPartition)) {
      throw new IllegalArgumentException("unknown topic partition: " + topicPartition);
    }
  }

  private long appendToLog(TopicPartition topicPartition, byte[] key, byte[] value) {
    try {
      // IOException belongs to the storage boundary; the broker API exposes domain operations.
      return logFor(topicPartition).append(key, value);
    } catch (IOException exception) {
      throw new IllegalStateException("failed to append to " + topicPartition, exception);
    }
  }

  private List<Message> readFromLog(TopicPartition topicPartition, long offset, int maxMessages) {
    try {
      // The storage implementation decides whether it reads from memory or from segment files.
      return logFor(topicPartition).readFrom(offset, maxMessages);
    } catch (IOException exception) {
      throw new IllegalStateException("failed to read from " + topicPartition, exception);
    }
  }

  private static List<TopicPartition> assignmentFor(
      List<TopicPartition> partitions, NavigableSet<String> members, String consumerId) {
    if (members == null || !members.contains(consumerId)) {
      return List.of();
    }

    /*
     * Deterministic round-robin assignment:
     *
     *   partitions: p0 p1 p2 p3
     *   members:    c0 c1
     *   result:     c0 gets p0,p2 and c1 gets p1,p3
     *
     * This is enough to show "one partition is owned by one group member at a time". It is not a
     * sticky assignor, and it does not try to minimize movement during membership changes.
     */
    List<String> orderedMembers = List.copyOf(members);
    int memberIndex = orderedMembers.indexOf(consumerId);
    List<TopicPartition> assignment = new ArrayList<>();
    for (int partitionIndex = 0; partitionIndex < partitions.size(); partitionIndex++) {
      if (partitionIndex % orderedMembers.size() == memberIndex) {
        assignment.add(partitions.get(partitionIndex));
      }
    }
    return List.copyOf(assignment);
  }

  private static void validateTopic(String topic) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
  }

  private static void validateGroupMember(String groupId, String consumerId) {
    if (groupId == null || groupId.isBlank()) {
      throw new IllegalArgumentException("groupId must not be blank");
    }
    if (consumerId == null || consumerId.isBlank()) {
      throw new IllegalArgumentException("consumerId must not be blank");
    }
  }

  /*
   * The real Kafka group key includes more coordinator state. For this lab, groupId + topic is the
   * smallest key that avoids mixing members of different single-topic groups.
   */
  private record GroupKey(String groupId, String topic) {}
}
