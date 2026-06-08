package lab.minikafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryKafkaBrokerTest {

  @Test
  void appendAssignsSequentialOffsetsPerPartition() {
    InMemoryKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 2);

    long firstOffset = broker.append("orders", 0, bytes("order-1"), bytes("created"));
    long secondOffset = broker.append("orders", 0, bytes("order-2"), bytes("paid"));
    long otherPartitionOffset = broker.append("orders", 1, bytes("order-3"), bytes("created"));

    assertEquals(0L, firstOffset);
    assertEquals(1L, secondOffset);
    assertEquals(0L, otherPartitionOffset);
    assertEquals(2L, broker.endOffset("orders", 0));
  }

  @Test
  void fetchReadsFromOffsetAndReturnsNextOffset() {
    InMemoryKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 1);
    broker.append("orders", 0, bytes("order-1"), bytes("created"));
    broker.append("orders", 0, bytes("order-2"), bytes("paid"));
    broker.append("orders", 0, bytes("order-3"), bytes("shipped"));

    FetchResult result = broker.fetch("orders", 0, 1, 2);

    List<Message> messages = result.messages();
    assertEquals(2, messages.size());
    assertEquals(3L, result.nextOffset());
    assertEquals(1L, messages.get(0).offset());
    assertArrayEquals(bytes("order-2"), messages.get(0).key());
    assertArrayEquals(bytes("paid"), messages.get(0).value());
    assertEquals(2L, messages.get(1).offset());
  }

  @Test
  void committedOffsetsAreTrackedPerConsumerGroup() {
    InMemoryKafkaBroker broker = new InMemoryKafkaBroker();
    broker.createTopic("orders", 1);
    broker.append("orders", 0, bytes("order-1"), bytes("created"));
    broker.append("orders", 0, bytes("order-2"), bytes("paid"));

    assertEquals(0L, broker.committedOffset("billing", "orders", 0));

    broker.commitOffset("billing", "orders", 0, 2L);
    broker.commitOffset("shipping", "orders", 0, 1L);

    assertEquals(2L, broker.committedOffset("billing", "orders", 0));
    assertEquals(1L, broker.committedOffset("shipping", "orders", 0));
  }

  @Test
  void unknownTopicPartitionFailsFast() {
    InMemoryKafkaBroker broker = new InMemoryKafkaBroker();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> broker.append("missing", 0, null, bytes("value")));

    assertEquals(
        "unknown topic partition: TopicPartition[topic=missing, partition=0]", error.getMessage());
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
