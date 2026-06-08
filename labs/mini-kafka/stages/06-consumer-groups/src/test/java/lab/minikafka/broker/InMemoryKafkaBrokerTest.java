package lab.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import lab.minikafka.api.FetchResult;
import lab.minikafka.api.MiniKafkaBroker;
import lab.minikafka.model.Message;
import org.junit.jupiter.api.Test;

class InMemoryKafkaBrokerTest {

  static {
    // Test logs are useful in this lab because they show append/fetch/commit flow while learning.
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
    System.setProperty("org.slf4j.simpleLogger.showShortLogName", "true");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "false");
  }

  @Test
  void appendAssignsSequentialOffsetsPerPartition() {
    // Two partitions means two independent logs, each with its own offset sequence.
    MiniKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 2);

    long firstOffset = broker.append("orders", 0, bytes("order-1"), bytes("created"));
    long secondOffset = broker.append("orders", 0, bytes("order-2"), bytes("paid"));
    long otherPartitionOffset = broker.append("orders", 1, bytes("order-3"), bytes("created"));

    assertEquals(0L, firstOffset);
    assertEquals(1L, secondOffset);

    // Partition 1 starts at offset 0 even though partition 0 already has records.
    assertEquals(0L, otherPartitionOffset);
    assertEquals(2L, broker.endOffset("orders", 0));
  }

  @Test
  void fetchReadsFromOffsetAndReturnsNextOffset() {
    // Seed one partition with three ordered records.
    MiniKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 1);
    broker.append("orders", 0, bytes("order-1"), bytes("created"));
    broker.append("orders", 0, bytes("order-2"), bytes("paid"));
    broker.append("orders", 0, bytes("order-3"), bytes("shipped"));

    FetchResult result = broker.fetch("orders", 0, 1, 2);

    // Reading from offset 1 skips the first record and returns at most two records.
    List<Message> messages = result.messages();
    assertEquals(2, messages.size());

    // The next offset is what a consumer would use for the next fetch or commit.
    assertEquals(3L, result.nextOffset());
    assertEquals(1L, messages.get(0).offset());
    assertArrayEquals(bytes("order-2"), messages.get(0).key());
    assertArrayEquals(bytes("paid"), messages.get(0).value());
    assertEquals(2L, messages.get(1).offset());
  }

  @Test
  void committedOffsetsAreTrackedPerConsumerGroup() {
    // The log is shared, but each group has independent progress.
    MiniKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 1);
    broker.append("orders", 0, bytes("order-1"), bytes("created"));
    broker.append("orders", 0, bytes("order-2"), bytes("paid"));

    // A group with no commit starts from the beginning of the partition.
    assertEquals(0L, broker.committedOffset("billing", "orders", 0));

    broker.commitOffset("billing", "orders", 0, 2L);
    broker.commitOffset("shipping", "orders", 0, 1L);

    assertEquals(2L, broker.committedOffset("billing", "orders", 0));
    assertEquals(1L, broker.committedOffset("shipping", "orders", 0));
  }

  @Test
  void unknownTopicPartitionFailsFast() {
    MiniKafkaBroker broker = new InMemoryKafkaBroker();

    // Failing before append avoids silently creating topics or partitions through producer writes.
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> broker.append("missing", 0, null, bytes("value")));

    assertEquals(
        "unknown topic partition: TopicPartition[topic=missing, partition=0]", error.getMessage());
  }

  private static byte[] bytes(String value) {
    // Tests store payloads as bytes to match Kafka's byte-oriented record model.
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
