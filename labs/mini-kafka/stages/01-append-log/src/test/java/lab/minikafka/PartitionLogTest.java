package lab.minikafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class PartitionLogTest {

  @Test
  void appendAssignsSequentialOffsets() {
    PartitionLog log = new PartitionLog();

    long firstOffset = log.append(bytes("order-1"), bytes("created"));
    long secondOffset = log.append(bytes("order-2"), bytes("paid"));

    assertEquals(0L, firstOffset);
    assertEquals(1L, secondOffset);
    assertEquals(2L, log.endOffset());
  }

  @Test
  void fetchReadsFromOffsetAndReturnsNextOffset() {
    PartitionLog log = new PartitionLog();
    log.append(bytes("order-1"), bytes("created"));
    log.append(bytes("order-2"), bytes("paid"));
    log.append(bytes("order-3"), bytes("shipped"));

    FetchResult result = log.fetch(1, 2);

    List<Message> messages = result.messages();
    assertEquals(2, messages.size());
    assertEquals(3L, result.nextOffset());
    assertEquals(1L, messages.get(0).offset());
    assertArrayEquals(bytes("order-2"), messages.get(0).key());
    assertArrayEquals(bytes("paid"), messages.get(0).value());
    assertEquals(2L, messages.get(1).offset());
  }

  @Test
  void negativeFetchOffsetFails() {
    PartitionLog log = new PartitionLog();

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> log.fetch(-1, 1));

    assertEquals("offset must be >= 0", error.getMessage());
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
