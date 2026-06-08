package lab.minikafka;

import java.util.List;
import java.util.Objects;

public record FetchResult(List<Message> messages, long nextOffset) {

  public FetchResult {
    Objects.requireNonNull(messages, "messages must not be null");
    messages = List.copyOf(messages);
    if (nextOffset < 0) {
      throw new IllegalArgumentException("nextOffset must be >= 0");
    }
  }
}
