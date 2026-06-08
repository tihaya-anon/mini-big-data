package lab.minikafka.consumer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lab.minikafka.api.FetchResult;
import lab.minikafka.api.MiniKafkaBroker;
import lab.minikafka.model.TopicPartition;

/**
 * Small single-topic consumer that joins a consumer group and tracks local fetch positions.
 *
 * <p>The broker owns committed offsets for the group. This consumer owns its current position: the
 * next offset it will fetch for each assigned partition. Polling advances the local position, and
 * {@link #commitSync()} copies those positions into the broker's committed group offsets.
 */
public final class MiniKafkaConsumer implements AutoCloseable {

  private final MiniKafkaBroker broker;
  private final String groupId;
  private final String consumerId;
  private final String topic;

  /*
   * Local fetch positions are intentionally separate from committed offsets. A consumer may poll
   * records and advance these positions before it commits; if it crashes in that window, the group
   * resumes from the older committed offsets.
   */
  private final Map<TopicPartition, Long> positions = new LinkedHashMap<>();

  private boolean closed;

  public MiniKafkaConsumer(
      MiniKafkaBroker broker, String groupId, String consumerId, String topic) {
    this.broker = Objects.requireNonNull(broker, "broker must not be null");
    this.groupId = requireText("groupId", groupId);
    this.consumerId = requireText("consumerId", consumerId);
    this.topic = requireText("topic", topic);

    /*
     * Construction behaves like "subscribe + join group" in a real Kafka client. The broker records
     * membership first, then the consumer initializes local positions from its assignment.
     */
    broker.joinConsumerGroup(this.groupId, this.consumerId, this.topic);
    refreshAssignment();
  }

  public String groupId() {
    return groupId;
  }

  public String consumerId() {
    return consumerId;
  }

  public String topic() {
    return topic;
  }

  /** Returns the current assignment and refreshes local positions for newly assigned partitions. */
  public synchronized List<TopicPartition> assignment() {
    ensureOpen();
    // Assignment can change after another consumer joins or leaves, so do not trust stale state.
    return refreshAssignment();
  }

  /**
   * Fetches records from each assigned partition and advances this consumer's local positions.
   *
   * <p>The returned map contains only partitions that produced records. Call {@link #commitSync()}
   * after processing records to make the new positions durable at the group level.
   */
  public synchronized Map<TopicPartition, FetchResult> poll(int maxMessagesPerPartition) {
    ensureOpen();
    if (maxMessagesPerPartition <= 0) {
      throw new IllegalArgumentException("maxMessagesPerPartition must be > 0");
    }

    List<TopicPartition> assignment = refreshAssignment();
    Map<TopicPartition, FetchResult> results = new LinkedHashMap<>();
    for (TopicPartition topicPartition : assignment) {
      // The local position is the offset of the next record this consumer will ask the broker for.
      long offset = positions.get(topicPartition);
      FetchResult result =
          broker.fetch(
              topicPartition.topic(), topicPartition.partition(), offset, maxMessagesPerPartition);

      /*
       * Polling advances local position immediately. Commit is a later decision made after the
       * caller has processed the records.
       */
      positions.put(topicPartition, result.nextOffset());
      if (!result.messages().isEmpty()) {
        // Empty fetches mean the consumer is caught up; callers usually do not need a map entry.
        results.put(topicPartition, result);
      }
    }
    return Collections.unmodifiableMap(results);
  }

  /** Commits this consumer's current local positions for all currently assigned partitions. */
  public synchronized void commitSync() {
    ensureOpen();

    /*
     * Refresh first so this consumer only commits partitions it currently owns. That keeps a
     * departed or reassigned consumer from overwriting progress for partitions it no longer owns.
     */
    refreshAssignment();
    for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      broker.commitOffset(
          groupId, topicPartition.topic(), topicPartition.partition(), entry.getValue());
    }
  }

  /** Returns this consumer's local next-fetch position for an assigned partition. */
  public synchronized long position(String topic, int partition) {
    ensureOpen();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    refreshAssignment();

    /*
     * Position is only meaningful for partitions this consumer owns. For unassigned partitions, the
     * broker's committed offset is still available through committedOffset.
     */
    Long position = positions.get(topicPartition);
    if (position == null) {
      throw new IllegalArgumentException("consumer is not assigned: " + topicPartition);
    }
    return position;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }

    /*
     * Leaving removes group membership but does not commit. That matches Kafka's separation between
     * lifecycle and processing acknowledgement.
     */
    broker.leaveConsumerGroup(groupId, consumerId, topic);
    positions.clear();
    closed = true;
  }

  private List<TopicPartition> refreshAssignment() {
    List<TopicPartition> assignment = broker.assignedPartitions(groupId, consumerId, topic);
    Set<TopicPartition> assignedPartitions = new LinkedHashSet<>(assignment);

    // Drop local positions for partitions that moved to another group member.
    positions.keySet().removeIf(topicPartition -> !assignedPartitions.contains(topicPartition));

    /*
     * New assignments start at the group's committed offset. That is the "resume after restart or
     * rebalance" behavior this mini consumer is meant to make explicit.
     */
    for (TopicPartition topicPartition : assignment) {
      positions.computeIfAbsent(topicPartition, this::committedOffset);
    }
    return assignment;
  }

  private long committedOffset(TopicPartition topicPartition) {
    return broker.committedOffset(groupId, topicPartition.topic(), topicPartition.partition());
  }

  private void ensureOpen() {
    // Every public operation except close requires active group membership.
    if (closed) {
      throw new IllegalStateException("consumer is closed");
    }
  }

  private static String requireText(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
