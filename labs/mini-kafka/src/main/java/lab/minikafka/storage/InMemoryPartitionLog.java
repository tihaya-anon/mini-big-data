package lab.minikafka.storage;

import java.util.ArrayList;
import java.util.List;
import lab.minikafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory append-only log used by the first milestone broker.
 */
public final class InMemoryPartitionLog implements PartitionLogStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryPartitionLog.class);

    private final List<Message> messages = new ArrayList<>();

    @Override
    public synchronized long append(byte[] key, byte[] value) {
        long offset = messages.size();
        messages.add(new Message(offset, key, value));
        LOG.debug("In-memory partition append completed at offset {}", offset);
        return offset;
    }

    /**
     * Reads a slice of the log starting from a known position.
     *
     * <p>This is the core Kafka read pattern: consumers keep track of an offset and ask for the
     * next batch beginning at that offset.
     */
    @Override
    public synchronized List<Message> readFrom(long offset, int maxMessages) {
        validateReadArguments(offset, maxMessages);
        if (offset >= messages.size()) {
            return List.of();
        }

        int start = Math.toIntExact(offset);
        int end = Math.min(messages.size(), start + maxMessages);
        LOG.debug("In-memory partition read returning records in range [{}:{})", start, end);
        return List.copyOf(messages.subList(start, end));
    }

    @Override
    public synchronized long endOffset() {
        return messages.size();
    }

    public static void validateReadArguments(long offset, int maxMessages) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be > 0");
        }
    }
}
