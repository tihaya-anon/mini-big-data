package lab.minikafka;

import java.util.ArrayList;
import java.util.List;

/**
 * One append-only log for a single topic-partition.
 *
 * <p>In real Kafka, a partition is persisted on disk and split into segments. Here it is just an
 * in-memory list so the relationship between append order and offsets stays obvious.
 */
public final class PartitionLog {

    private final List<Message> messages = new ArrayList<>();

    public synchronized long append(byte[] key, byte[] value) {
        long offset = messages.size();
        messages.add(new Message(offset, key, value));
        return offset;
    }

    /**
     * Reads a slice of the log starting from a known position.
     *
     * <p>This is the core Kafka read pattern: consumers keep track of an offset and ask for the
     * next batch beginning at that offset.
     */
    public synchronized List<Message> readFrom(long offset, int maxMessages) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be > 0");
        }
        if (offset >= messages.size()) {
            return List.of();
        }

        int start = Math.toIntExact(offset);
        int end = Math.min(messages.size(), start + maxMessages);
        return List.copyOf(messages.subList(start, end));
    }

    public synchronized long endOffset() {
        return messages.size();
    }
}
