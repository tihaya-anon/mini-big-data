package lab.minikafka;

import java.util.List;

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
