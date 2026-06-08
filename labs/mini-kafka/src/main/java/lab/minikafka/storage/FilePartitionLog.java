package lab.minikafka.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import lab.minikafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File-backed append-only log for a single partition.
 *
 * <p>Unlike the previous single-file version, this implementation stores records in multiple
 * segment files. Segmenting solves a practical problem: a real log grows without bound, so Kafka
 * needs manageable chunks for rollover, recovery, retention, and later indexing.
 */
public final class FilePartitionLog implements PartitionLogStore {

  private static final Logger LOG = LoggerFactory.getLogger(FilePartitionLog.class);
  private static final String SEGMENT_SUFFIX = ".log";
  private static final int DEFAULT_MAX_RECORDS_PER_SEGMENT = 3;

  private final Path partitionDirectory;
  private final int maxRecordsPerSegment;
  private final List<LogSegment> segments;
  private long nextOffset;

  public FilePartitionLog(Path partitionDirectory) throws IOException {
    this(partitionDirectory, DEFAULT_MAX_RECORDS_PER_SEGMENT);
  }

  public FilePartitionLog(Path partitionDirectory, int maxRecordsPerSegment) throws IOException {
    if (maxRecordsPerSegment <= 0) {
      throw new IllegalArgumentException("maxRecordsPerSegment must be > 0");
    }
    this.partitionDirectory = partitionDirectory;
    this.maxRecordsPerSegment = maxRecordsPerSegment;
    Files.createDirectories(partitionDirectory);
    this.segments = loadSegments();
    this.nextOffset = segments.isEmpty() ? 0 : segments.getLast().nextOffset();

    if (segments.isEmpty()) {
      segments.add(createSegment(0));
    }

    LOG.info(
        "Opened segmented partition log {} with {} segment(s) and end offset {}",
        partitionDirectory,
        segments.size(),
        nextOffset);
  }

  @Override
  public synchronized long append(byte[] key, byte[] value) throws IOException {
    LogSegment activeSegment = activeSegment();
    if (activeSegment.recordCount() >= maxRecordsPerSegment) {
      activeSegment = rollSegment();
    }

    long offset = nextOffset;
    activeSegment.append(key, value);
    nextOffset++;
    LOG.debug(
        "Segmented file append completed at offset {} in segment {}",
        offset,
        activeSegment.path().getFileName());
    return offset;
  }

  @Override
  public synchronized List<Message> readFrom(long offset, int maxMessages) throws IOException {
    InMemoryPartitionLog.validateReadArguments(offset, maxMessages);
    if (offset >= nextOffset) {
      return List.of();
    }

    List<Message> messages = new ArrayList<>();
    for (LogSegment segment : segments) {
      if (segment.nextOffset() <= offset) {
        continue;
      }
      messages.addAll(segment.readFrom(offset, maxMessages - messages.size()));
      if (messages.size() == maxMessages) {
        break;
      }
    }

    LOG.debug(
        "Segmented file read returned {} record(s) from {} starting at offset {}",
        messages.size(),
        partitionDirectory,
        offset);
    return List.copyOf(messages);
  }

  @Override
  public synchronized long endOffset() {
    return nextOffset;
  }

  private LogSegment activeSegment() {
    return segments.getLast();
  }

  private LogSegment rollSegment() throws IOException {
    LogSegment segment = createSegment(nextOffset);
    segments.add(segment);
    LOG.info("Rolled new segment {} at base offset {}", segment.path().getFileName(), nextOffset);
    return segment;
  }

  private List<LogSegment> loadSegments() throws IOException {
    List<LogSegment> loadedSegments = new ArrayList<>();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(partitionDirectory, "*" + SEGMENT_SUFFIX)) {
      for (Path path : stream) {
        loadedSegments.add(LogSegment.openExisting(path));
      }
    }
    loadedSegments.sort(Comparator.comparingLong(LogSegment::baseOffset));
    return loadedSegments;
  }

  private LogSegment createSegment(long baseOffset) throws IOException {
    Path path = partitionDirectory.resolve(segmentFileName(baseOffset));
    return LogSegment.createNew(path, baseOffset);
  }

  private static String segmentFileName(long baseOffset) {
    return String.format("%020d%s", baseOffset, SEGMENT_SUFFIX);
  }

  private static final class LogSegment {

    private final Path path;
    private final long baseOffset;
    private int recordCount;

    private LogSegment(Path path, long baseOffset, int recordCount) {
      this.path = path;
      this.baseOffset = baseOffset;
      this.recordCount = recordCount;
    }

    static LogSegment createNew(Path path, long baseOffset) throws IOException {
      if (!Files.exists(path)) {
        Files.createFile(path);
      }
      return new LogSegment(path, baseOffset, 0);
    }

    static LogSegment openExisting(Path path) throws IOException {
      long baseOffset = parseBaseOffset(path);
      int recordCount = countRecords(path);
      return new LogSegment(path, baseOffset, recordCount);
    }

    void append(byte[] key, byte[] value) throws IOException {
      try (DataOutputStream output =
          new DataOutputStream(
              new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.APPEND)))) {
        writeRecord(output, key, value);
      }
      recordCount++;
    }

    List<Message> readFrom(long offset, int maxMessages) throws IOException {
      if (maxMessages <= 0 || offset >= nextOffset()) {
        return List.of();
      }

      List<Message> messages = new ArrayList<>();
      try (DataInputStream input =
          new DataInputStream(
              new BufferedInputStream(Files.newInputStream(path, StandardOpenOption.READ)))) {
        long currentOffset = baseOffset;
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
        return messages;
      }
      return messages;
    }

    long nextOffset() {
      return baseOffset + recordCount;
    }

    Path path() {
      return path;
    }

    long baseOffset() {
      return baseOffset;
    }

    int recordCount() {
      return recordCount;
    }

    private static long parseBaseOffset(Path path) {
      String fileName = path.getFileName().toString();
      String baseOffsetText = fileName.substring(0, fileName.length() - SEGMENT_SUFFIX.length());
      return Long.parseLong(baseOffsetText);
    }

    private static int countRecords(Path path) throws IOException {
      int count = 0;
      try (DataInputStream input =
          new DataInputStream(
              new BufferedInputStream(Files.newInputStream(path, StandardOpenOption.READ)))) {
        while (true) {
          readRecord(input);
          count++;
        }
      } catch (EOFException ignored) {
        return count;
      }
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
