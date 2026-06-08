package lab.minikafka.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class MessageTest {

  @Test
  void copiesPayloadsOnConstructionAndAccess() {
    byte[] key = bytes("order-1");
    byte[] value = bytes("created");
    Message message = new Message(0, key, value);

    key[0] = 'x';
    value[0] = 'y';

    assertArrayEquals(bytes("order-1"), message.key());
    assertArrayEquals(bytes("created"), message.value());

    byte[] returnedKey = message.key();
    returnedKey[0] = 'z';

    assertArrayEquals(bytes("order-1"), message.key());
  }

  @Test
  void comparesPayloadsByContent() {
    Message message = new Message(1, bytes("order-2"), bytes("paid"));
    Message samePayload = new Message(1, bytes("order-2"), bytes("paid"));

    assertEquals(message, samePayload);
    assertEquals(message.hashCode(), samePayload.hashCode());
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
