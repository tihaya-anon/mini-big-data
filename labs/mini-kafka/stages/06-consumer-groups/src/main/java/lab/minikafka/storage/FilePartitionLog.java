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
 * File-backed append-only log for one topic partition.
 *
 * <p>The first storage milestone, {@link InMemoryPartitionLog}, shows the simplest Kafka idea:
 * append records and let consumers read by offset. This class keeps the same contract but adds the
 * next production concern: persistence. Records are written to files, and startup rebuilds the
 * log's end offset by inspecting those files.
 *
 * <p>The log is split into segment files instead of keeping one large file forever. A segment is a
 * contiguous range of offsets with a base offset encoded in its filename, for example {@code
 * 00000000000000000042.log}. Segmenting is the foundation for Kafka features such as rollover,
 * retention, compaction, and sparse indexes. This mini implementation only demonstrates rollover
 * and recovery; it intentionally leaves indexing, deletion, checksums, and fsync policy for later
 * labs.
 */
public final class FilePartitionLog implements PartitionLogStore {

  private static final Logger LOG = LoggerFactory.getLogger(FilePartitionLog.class);
  private static final String SEGMENT_SUFFIX = ".log";

  /**
   * Kept deliberately small so tests and examples can show segment rollover after only a few
   * appends. A real broker would size segments by bytes and time, not by record count.
   */
  private static final int DEFAULT_MAX_RECORDS_PER_SEGMENT = 3;

  private final Path partitionDirectory;
  private final int maxRecordsPerSegment;

  /*
   * Segment order is the partition order. The last segment is the only segment append writes to;
   * earlier segments are read-only in this milestone.
   */
  private final List<LogSegment> segments;

  /*
   * nextOffset is kept at the outer partition level because offsets are global within a partition,
   * not local to individual segment files.
   */
  private long nextOffset;

  public FilePartitionLog(Path partitionDirectory) throws IOException {
    this(partitionDirectory, DEFAULT_MAX_RECORDS_PER_SEGMENT);
  }

  /**
   * Opens or creates a partition log rooted at {@code partitionDirectory}.
   *
   * <p>Startup recovery has two steps:
   *
   * <ol>
   *   <li>Load every {@code *.log} segment and sort by base offset.
   *   <li>Recover the end offset from the last segment's base offset plus its record count.
   * </ol>
   *
   * <p>This is intentionally simpler than Kafka. Kafka also stores indexes and validates record
   * batches. Here, scanning each segment keeps the persistence format visible for students.
   */
  public FilePartitionLog(Path partitionDirectory, int maxRecordsPerSegment) throws IOException {
    if (maxRecordsPerSegment <= 0) {
      throw new IllegalArgumentException("maxRecordsPerSegment must be > 0");
    }
    this.partitionDirectory = partitionDirectory;
    this.maxRecordsPerSegment = maxRecordsPerSegment;

    // The directory layout mirrors Kafka's topic/partition storage boundary.
    Files.createDirectories(partitionDirectory);

    // Existing segment files are the source of truth during startup recovery.
    this.segments = loadSegments();
    this.nextOffset = segments.isEmpty() ? 0 : segments.getLast().nextOffset();

    // A new empty partition still needs one active segment ready for appends.
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

    // Rollover happens before writing so every segment stays within the configured record limit.
    if (activeSegment.recordCount() >= maxRecordsPerSegment) {
      activeSegment = rollSegment();
    }

    /*
     * Capture the current partition end as the assigned offset, append bytes to disk, then advance
     * the end offset. The file does not store this offset explicitly.
     */
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

    // An offset equal to the partition end is a valid empty fetch.
    if (offset >= nextOffset) {
      return List.of();
    }

    /*
     * Offsets are global to the partition, not local to a file. Each segment decides whether it can
     * contribute records for the requested offset range, then the outer log stops once the requested
     * batch size is satisfied.
     */
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
    // The active segment is always the newest segment in sorted segment order.
    return segments.getLast();
  }

  private LogSegment rollSegment() throws IOException {
    // The new file name starts at the next partition offset, which becomes the segment base offset.
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
        // Opening a segment also counts its records, which recovers that segment's end offset.
        loadedSegments.add(LogSegment.openExisting(path));
      }
    }

    // File systems do not promise directory iteration order, so sort by base offset explicitly.
    loadedSegments.sort(Comparator.comparingLong(LogSegment::baseOffset));
    return loadedSegments;
  }

  private LogSegment createSegment(long baseOffset) throws IOException {
    // The base offset in the filename is the bridge from physical files back to logical offsets.
    Path path = partitionDirectory.resolve(segmentFileName(baseOffset));
    return LogSegment.createNew(path, baseOffset);
  }

  private static String segmentFileName(long baseOffset) {
    return String.format("%020d%s", baseOffset, SEGMENT_SUFFIX);
  }

  /**
   * One physical segment file inside a partition log.
   *
   * <p>This is a private static nested class because a segment is an implementation detail of
   * {@link FilePartitionLog}: callers should reason about partition offsets, not about segment
   * files. Keeping it nested also makes the boundary visible: the outer class owns partition-level
   * ordering and rollover, while this class owns a single file's base offset, record count, append,
   * and scan behavior.
   *
   * <p>The class is {@code static} so it does not capture the outer {@code FilePartitionLog}
   * instance. That keeps construction explicit and prevents accidental access to partition-level
   * state such as {@code nextOffset}. If segment behavior grows to include indexes, retention, or
   * compaction, it should become a package-private top-level class with its own tests.
   */
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
      // createTopic on an empty partition creates the file; reopening uses openExisting instead.
      if (!Files.exists(path)) {
        Files.createFile(path);
      }
      return new LogSegment(path, baseOffset, 0);
    }

    static LogSegment openExisting(Path path) throws IOException {
      // Both base offset and record count are needed to know the segment's covered offset range.
      long baseOffset = parseBaseOffset(path);
      int recordCount = countRecords(path);
      return new LogSegment(path, baseOffset, recordCount);
    }

    /**
     * Appends one record to this segment file.
     *
     * <p>The offset is deliberately not stored in the record body. The segment's base offset and
     * the record's position within the file are enough to reconstruct offsets during reads. That
     * mirrors the log-structured idea: physical order is part of the data model.
     */
    void append(byte[] key, byte[] value) throws IOException {
      try (DataOutputStream output =
          new DataOutputStream(
              new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.APPEND)))) {
        // Append writes exactly one length-prefixed key/value pair to the tail of the segment file.
        writeRecord(output, key, value);
      }

      // The in-memory count is the segment-local end offset relative to baseOffset.
      recordCount++;
    }

    /**
     * Reads records from this segment that are at or after the requested partition offset.
     *
     * <p>The method scans from the beginning of the segment because this lab does not have offset
     * indexes yet. That makes reads O(segment size), which is acceptable for a teaching
     * implementation and exposes why Kafka keeps index files next to log segment files.
     */
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
          /*
           * Reading records advances currentOffset one by one. Until indexes exist, the only way to
           * skip to a later offset is to scan through earlier records.
           */
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
      // Segment end is base offset plus the number of complete records found in this file.
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
      // Segment names are fixed-width decimal offsets plus ".log".
      String fileName = path.getFileName().toString();
      String baseOffsetText = fileName.substring(0, fileName.length() - SEGMENT_SUFFIX.length());
      return Long.parseLong(baseOffsetText);
    }

    /**
     * Counts complete records during startup recovery.
     *
     * <p>EOF is the normal terminator for this compact binary format. A production log would also
     * detect torn writes with record batch lengths and checksums; this lab keeps the format small
     * so the segment mechanics stay readable.
     */
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
    // The record format is just two nullable byte arrays: key first, value second.
    writeNullableBytes(output, key);
    writeNullableBytes(output, value);
  }

  /**
   * Reads one record from the current stream position.
   *
   * <p>The on-disk format is intentionally tiny:
   *
   * <ol>
   *   <li>key length as a 4-byte signed integer, or {@code -1} for a null key
   *   <li>key bytes, if present
   *   <li>value length as a 4-byte signed integer, or {@code -1} for a null value
   *   <li>value bytes, if present
   * </ol>
   *
   * <p>The record has no timestamp, magic byte, checksum, compression flag, or batch header. Those
   * are real Kafka concerns, but omitting them keeps this lab focused on append-only persistence
   * and offset recovery.
   */
  private static RecordBytes readRecord(DataInputStream input) throws IOException {
    byte[] key = readNullableBytes(input);
    byte[] value = readNullableBytes(input);
    return new RecordBytes(key, value);
  }

  private static void writeNullableBytes(DataOutputStream output, byte[] data) throws IOException {
    if (data == null) {
      // -1 is the sentinel for null; zero is reserved for an empty but present byte array.
      output.writeInt(-1);
      return;
    }

    // Length-prefixing lets reads know exactly how many bytes belong to this field.
    output.writeInt(data.length);
    output.write(data);
  }

  private static byte[] readNullableBytes(DataInputStream input) throws IOException {
    int length = input.readInt();
    if (length < 0) {
      return null;
    }

    // readFully either fills the whole array or throws EOFException for an incomplete tail record.
    byte[] data = new byte[length];
    input.readFully(data);
    return data;
  }

  private record RecordBytes(byte[] key, byte[] value) {

    private RecordBytes {
      // Defensive copies keep callers from mutating bytes after a record has been read.
      key = key == null ? null : Arrays.copyOf(key, key.length);
      value = value == null ? null : Arrays.copyOf(value, value.length);
    }
  }
}
