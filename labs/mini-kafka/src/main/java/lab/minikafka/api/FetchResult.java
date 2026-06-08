package lab.minikafka.api;

import java.util.List;
import lab.minikafka.model.Message;

/**
 * Result of reading from a partition log.
 *
 * <p>The next offset is returned explicitly because Kafka consumers typically feed that value into
 * the next fetch or into offset commit logic.
 */
public final class FetchResult {

  private final List<Message> messages;
  private final long nextOffset;

  public FetchResult(List<Message> messages, long nextOffset) {
    this.messages = List.copyOf(messages);
    this.nextOffset = nextOffset;
  }

  public List<Message> messages() {
    return messages;
  }

  public long nextOffset() {
    return nextOffset;
  }
}
