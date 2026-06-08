package lab.minikafka.model;

/**
 * Identifies one ordered log inside Kafka.
 *
 * <p>Topics are split into partitions because Kafka scales by having multiple independent logs
 * rather than one globally ordered stream.
 */
public record TopicPartition(String topic, int partition) {

  public TopicPartition {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("partition must be >= 0");
    }
  }
}
