package lab.minikafka.api;

import java.util.List;
import lab.minikafka.model.Message;

/**
 * Result of reading from a partition log.
 *
 * <p>The next offset is returned explicitly because Kafka consumers typically feed that value into
 * the next fetch or into offset commit logic.
 */
public record FetchResult(List<Message> messages, long nextOffset) {

  public FetchResult {
    // Copy the list so callers cannot mutate the broker's fetch result after construction.
    messages = List.copyOf(messages);
  }
}
