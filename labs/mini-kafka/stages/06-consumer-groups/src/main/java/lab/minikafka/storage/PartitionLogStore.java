package lab.minikafka.storage;

import java.io.IOException;
import java.util.List;
import lab.minikafka.model.Message;

/**
 * Storage contract for one topic-partition log.
 *
 * <p>The broker layer owns topic metadata and consumer group offsets. Implementations of this
 * interface only own record order inside one partition: append assigns the next offset, read
 * returns records from a requested offset, and endOffset reports the next append position.
 */
public interface PartitionLogStore {

  /**
   * Appends one record to the end of this partition log.
   *
   * @return the partition offset assigned to the record
   */
  long append(byte[] key, byte[] value) throws IOException;

  /**
   * Reads records from this partition log starting at {@code offset}.
   *
   * <p>Returning an empty list for {@code offset == endOffset()} is normal and means the consumer
   * has caught up. Invalid negative offsets and non-positive batch sizes are rejected by
   * implementations.
   */
  List<Message> readFrom(long offset, int maxMessages) throws IOException;

  /**
   * Returns the next offset that will be assigned by {@link #append(byte[], byte[])}.
   *
   * <p>Because this lab does not implement retention yet, this is also the current record count.
   */
  long endOffset();
}
