package lab.minikafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lab.minikafka.api.FetchResult;
import lab.minikafka.api.MiniKafkaBroker;
import lab.minikafka.broker.InMemoryKafkaBroker;
import lab.minikafka.model.Message;
import lab.minikafka.model.TopicPartition;
import org.junit.jupiter.api.Test;

class MiniKafkaConsumerTest {

  @Test
  void pollStartsAtCommittedOffsetAndCommitStoresLocalPosition() {
    // The group has already processed offset 0, so its committed next offset is 1.
    MiniKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 1);
    broker.append("orders", 0, bytes("order-1"), bytes("created"));
    broker.append("orders", 0, bytes("order-2"), bytes("paid"));
    broker.append("orders", 0, bytes("order-3"), bytes("shipped"));
    broker.commitOffset("billing", "orders", 0, 1L);

    try (MiniKafkaConsumer consumer =
        new MiniKafkaConsumer(broker, "billing", "consumer-a", "orders")) {
      Map<TopicPartition, FetchResult> records = consumer.poll(2);

      TopicPartition partition = topicPartition(0);
      assertEquals(Set.of(partition), records.keySet());

      // Poll starts from committed offset 1 and advances the local position to 3.
      assertEquals(List.of(1L, 2L), offsets(records.get(partition).messages()));
      assertEquals(3L, consumer.position("orders", 0));

      // Polling alone does not commit; the group offset is still the old value.
      assertEquals(1L, broker.committedOffset("billing", "orders", 0));

      consumer.commitSync();

      assertEquals(3L, broker.committedOffset("billing", "orders", 0));
    }
  }

  @Test
  void consumersInSameGroupSplitTopicPartitions() {
    // Two partitions and two consumers should produce one assigned partition per consumer.
    MiniKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 2);
    broker.append("orders", 0, bytes("order-1"), bytes("created"));
    broker.append("orders", 1, bytes("order-2"), bytes("paid"));

    try (MiniKafkaConsumer first =
            new MiniKafkaConsumer(broker, "workers", "consumer-a", "orders");
        MiniKafkaConsumer second =
            new MiniKafkaConsumer(broker, "workers", "consumer-b", "orders")) {
      TopicPartition firstPartition = topicPartition(0);
      TopicPartition secondPartition = topicPartition(1);

      assertEquals(List.of(firstPartition), first.assignment());
      assertEquals(List.of(secondPartition), second.assignment());

      // Because assignments are disjoint, each consumer polls a different partition.
      assertEquals(Set.of(firstPartition), first.poll(10).keySet());
      assertEquals(Set.of(secondPartition), second.poll(10).keySet());
    }
  }

  @Test
  void leavingGroupReassignsPartitionsToRemainingConsumers() {
    // Start with two consumers so the topic's partitions are split.
    MiniKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 2);

    try (MiniKafkaConsumer first =
        new MiniKafkaConsumer(broker, "workers", "consumer-a", "orders")) {
      MiniKafkaConsumer second = new MiniKafkaConsumer(broker, "workers", "consumer-b", "orders");

      assertEquals(List.of(topicPartition(0)), first.assignment());

      second.close();

      // Once the second consumer leaves, the first consumer owns both partitions.
      assertEquals(List.of(topicPartition(0), topicPartition(1)), first.assignment());
    }
  }

  @Test
  void positionFailsForUnassignedPartition() {
    // This guards against accidentally reading or committing a partition owned by another member.
    MiniKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 2);

    try (MiniKafkaConsumer first =
            new MiniKafkaConsumer(broker, "workers", "consumer-a", "orders");
        MiniKafkaConsumer second =
            new MiniKafkaConsumer(broker, "workers", "consumer-b", "orders")) {
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> first.position("orders", 1));

      assertEquals(
          "consumer is not assigned: TopicPartition[topic=orders, partition=1]",
          error.getMessage());
    }
  }

  private static List<Long> offsets(List<Message> messages) {
    // Tests assert offsets directly because offsets are the core Kafka learning target here.
    return messages.stream().map(Message::offset).toList();
  }

  private static TopicPartition topicPartition(int partition) {
    // All consumer tests use the same topic; only the partition number changes.
    return new TopicPartition("orders", partition);
  }

  private static byte[] bytes(String value) {
    // The consumer API deals in broker records, which carry byte-array payloads.
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
