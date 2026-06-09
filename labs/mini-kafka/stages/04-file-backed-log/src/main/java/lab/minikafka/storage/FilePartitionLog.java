package lab.minikafka.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lab.minikafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File-backed append-only log for a single partition.
 *
 * <p>This keeps the storage format intentionally simple: records are written sequentially into one
 * file. The goal of this milestone is persistence and restart behavior, not segment management.
 */
public final class FilePartitionLog implements PartitionLogStore {

  private static final Logger LOG = LoggerFactory.getLogger(FilePartitionLog.class);

  private final Path logFile;
  private long nextOffset;

  public FilePartitionLog(Path logFile) throws IOException {
    this.logFile = logFile;
    Files.createDirectories(logFile.getParent());
    if (!Files.exists(logFile)) {
      Files.createFile(logFile);
    }
    this.nextOffset = countRecords();
    LOG.info("Opened file-backed partition log {} with end offset {}", logFile, nextOffset);
  }

  @Override
  public synchronized long append(byte[] key, byte[] value) throws IOException {
    long offset = nextOffset;
    try (DataOutputStream output =
        new DataOutputStream(
            new BufferedOutputStream(Files.newOutputStream(logFile, StandardOpenOption.APPEND)))) {
      writeRecord(output, key, value);
    }
    nextOffset++;
    LOG.debug("File partition append completed at offset {} for {}", offset, logFile);
    return offset;
  }

  @Override
  public synchronized List<Message> readFrom(long offset, int maxMessages) throws IOException {
    InMemoryPartitionLog.validateReadArguments(offset, maxMessages);
    if (offset >= nextOffset) {
      return List.of();
    }

    List<Message> messages = new ArrayList<>();
    /*
     * This stage has no index, so reading offset N means scanning from the beginning and counting
     * records. Stage 05 changes file layout, but indexing is still intentionally left for later.
     */
    try (DataInputStream input =
        new DataInputStream(
            new BufferedInputStream(Files.newInputStream(logFile, StandardOpenOption.READ)))) {
      long currentOffset = 0;
      while (true) {
        RecordBytes record = readRecord(input);
        if (currentOffset >= offset && messages.size() < maxMessages) {
          messages.add(new Message(currentOffset, record.key(), record.value()));
        }
        currentOffset++;
        if (messages.size() == maxMessages) {
          break;
        }
      }
    } catch (EOFException ignored) {
      // End of file is the normal stop condition.
    }

    LOG.debug(
        "File partition read returned {} record(s) from {} starting at offset {}",
        messages.size(),
        logFile,
        offset);
    return List.copyOf(messages);
  }

  @Override
  public synchronized long endOffset() {
    return nextOffset;
  }

  private long countRecords() throws IOException {
    long count = 0;
    /*
     * Recovery is intentionally simple: replay the file shape and count complete records. A real
     * broker also needs checksums and truncation rules for partial tail writes.
     */
    try (DataInputStream input =
        new DataInputStream(
            new BufferedInputStream(Files.newInputStream(logFile, StandardOpenOption.READ)))) {
      while (true) {
        readRecord(input);
        count++;
      }
    } catch (EOFException ignored) {
      return count;
    }
  }

  private static void writeRecord(DataOutputStream output, byte[] key, byte[] value)
      throws IOException {
    writeNullableBytes(output, key);
    writeNullableBytes(output, value);
  }

  private static RecordBytes readRecord(DataInputStream input) throws IOException {
    byte[] key = readNullableBytes(input);
    byte[] value = readNullableBytes(input);
    return new RecordBytes(key, value);
  }

  private static void writeNullableBytes(DataOutputStream output, byte[] data) throws IOException {
    // -1 is a tiny nullable marker; non-null values are length-prefixed bytes.
    if (data == null) {
      output.writeInt(-1);
      return;
    }
    output.writeInt(data.length);
    output.write(data);
  }

  private static byte[] readNullableBytes(DataInputStream input) throws IOException {
    int length = input.readInt();
    if (length < 0) {
      return null;
    }
    byte[] data = new byte[length];
    input.readFully(data);
    return data;
  }

  private record RecordBytes(byte[] key, byte[] value) {

    private RecordBytes {
      key = key == null ? null : Arrays.copyOf(key, key.length);
      value = value == null ? null : Arrays.copyOf(value, value.length);
    }
  }
}
