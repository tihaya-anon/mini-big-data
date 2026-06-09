package lab.minikafka.storage;

import java.io.IOException;
import java.util.List;
import lab.minikafka.model.Message;

/**
 * Storage contract for one topic-partition log.
 *
 * <p>The broker should not care whether records live in memory or on disk. This interface is the
 * first place where the lab separates the Kafka API shape from the storage mechanism behind it.
 */
public interface PartitionLogStore {

  /** Appends one record and returns the offset assigned by this partition log. */
  long append(byte[] key, byte[] value) throws IOException;

  /** Reads records by logical offset even if the implementation has to scan physical bytes. */
  List<Message> readFrom(long offset, int maxMessages) throws IOException;

  /** Returns the next offset that would be assigned to a newly appended record. */
  long endOffset();
}
