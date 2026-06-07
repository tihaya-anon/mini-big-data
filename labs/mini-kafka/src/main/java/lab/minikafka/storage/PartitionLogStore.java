package lab.minikafka.storage;

import java.io.IOException;
import java.util.List;
import lab.minikafka.model.Message;

/**
 * Storage contract for one topic-partition log.
 */
public interface PartitionLogStore {

    long append(byte[] key, byte[] value) throws IOException;

    List<Message> readFrom(long offset, int maxMessages) throws IOException;

    long endOffset();
}
