package lab.minikafka;

import java.util.Objects;

/**
 * Identifies one ordered log inside Kafka.
 *
 * <p>Topics are split into partitions because Kafka scales by having multiple independent logs
 * rather than one globally ordered stream.
 */
public final class TopicPartition {

  private final String topic;
  private final int partition;

  public TopicPartition(String topic, int partition) {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("partition must be >= 0");
    }
    this.topic = topic;
    this.partition = partition;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TopicPartition)) {
      return false;
    }
    TopicPartition that = (TopicPartition) other;
    return partition == that.partition && topic.equals(that.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition);
  }

  @Override
  public String toString() {
    return "TopicPartition[topic=" + topic + ", partition=" + partition + "]";
  }
}
