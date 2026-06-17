# mini Kafka 源码阅读参考答案

这份答案用于核对 `labs/mini-kafka/exercises/source-reading-report.md`。答案不要求逐字一致，但需要覆盖相同关键点，并能从源码或测试中找到证据。

## A. Stage 01: append-only log

1. `PartitionLog.append` 使用当前 `messages.size()` 作为新消息 offset，然后把 `new Message(offset, key, value)` 追加到 `messages`。空列表的 size 是 `0`，所以第一条消息 offset 为 `0`。证据：`labs/mini-kafka/stages/01-append-log/src/main/java/lab/minikafka/PartitionLog.java`。

2. `FetchResult.nextOffset` 表示下一次 fetch 应该从哪里继续读，也可以在处理完成后作为提交进度。它不是最后一条消息 offset，因为读取 offset `1` 和两条消息后，最后一条消息 offset 是 `2`，下一次要读的是 `3`。证据：`PartitionLog.fetch` 里的 `offset + batch.size()`，以及 Stage 02/03/06 的 `fetchReadsFromOffsetAndReturnsNextOffset` 测试。

3. 如果请求 offset 等于或大于当前消息数量，`readFrom` 返回空列表，`nextOffset` 会是请求的 offset 加 `0`。这说明当前没有 retention gap、offset reset、log start offset、越界校正等行为。证据：`PartitionLog.readFrom` 的 `if (offset >= messages.size()) return List.of();`。

## B. Stage 02: topics and partitions

4. Stage 02 用 `Map<TopicPartition, PartitionLog>` 表示 broker 内的日志集合。`createTopic` 为每个 partition 创建一个 `TopicPartition` key 和一个独立 `PartitionLog`。证据：`labs/mini-kafka/stages/02-topic-partitions/src/main/java/lab/minikafka/InMemoryKafkaBroker.java`。

5. offset 是 partition 内的位置，不是 topic 全局序号。每个 partition 对应自己的 `PartitionLog`，所以 partition 0 的第一条记录是 offset `0`，partition 1 的第一条记录也是 offset `0`。证据：Stage 02 的 `appendAssignsSequentialOffsetsPerPartition` 测试。

6. 写入不存在的 topic partition 会抛出 `IllegalArgumentException`，错误来自 `logFor` 找不到 `TopicPartition`。这让 topic/partition 必须显式创建，避免 producer 写入时悄悄创建错误的日志。证据：`logFor` 和 `unknownTopicPartitionFailsFast` 测试。

## C. Stage 03: committed offsets

7. `commitOffset` 存的是下一次要读的 offset。处理完 offsets `0` 和 `1` 后应提交 `2`，这在代码注释和 `committedOffsetsAreTrackedPerConsumerGroup` 测试中都有体现。证据：`labs/mini-kafka/stages/03-consumer-offsets/src/main/java/lab/minikafka/InMemoryKafkaBroker.java`。

8. `groupOffsets` 是 `Map<String, Map<TopicPartition, Long>>`，第一层 key 是 group ID。不同 group 的进度写入不同内层 map，因此它们可以共享同一物理 log，但保存不同 committed offset。证据：同一测试里 `billing` 提交 `2`、`shipping` 提交 `1`。

9. 从未提交过 offset 的 group 返回 `0L`，也就是从 partition 开头读。真实 Kafka 里类似 `auto.offset.reset=earliest` 的行为；本 lab 把它硬编码为 earliest。证据：`committedOffset` 使用 `getOrDefault(..., 0L)`。

10. Stage 03 完全是 in-memory broker，`groupOffsets` 只是内存 map，不会随消息持久化。持久化从 Stage 04 才开始，并且 Stage 04/06 也只持久化 record data，不持久化 group offsets。证据：Stage 03 `InMemoryKafkaBroker` 和 `labs/mini-kafka/notes/scope.md` 的 persistence boundary。

## D. Stage 04: storage abstraction and file-backed log

11. `PartitionLogStore` 抽象出 `append`、`readFrom`、`endOffset`，broker 只负责 topic partition 查找、append/fetch API、offset/group 状态等流程；具体是 ArrayList 还是文件由 storage 实现决定。证据：`labs/mini-kafka/stages/04-file-backed-log/src/main/java/lab/minikafka/storage/PartitionLogStore.java` 和 `AbstractSingleNodeKafkaBroker.java`。

12. 记录格式是两个 nullable byte arrays：先写 key length，`-1` 表示 null，否则写 key bytes；再写 value length，`-1` 表示 null，否则写 value bytes。长度是 `DataOutputStream.writeInt` 写出的 4-byte signed integer。证据：Stage 04 `FilePartitionLog.writeRecord`、`writeNullableBytes`、`readRecord`。

13. 重启恢复时，`FilePartitionLog` 打开已有文件，调用 `countRecords()` 从头读取完整 record 并计数，把计数结果作为 `nextOffset`。新 broker 对同一目录再次 `createTopic` 时会重新打开 log。证据：Stage 04 `FilePartitionLog` 构造函数、`countRecords`，以及 `recordsSurviveBrokerRestart`、`appendContinuesFromRecoveredEndOffset` 测试。

14. Stage 04 没有 offset index，record 本身也不存 offset，所以从 offset N 读取只能从文件头开始逐条扫描并计数到目标位置。下一个问题是需要 segment 和 index 来限制文件大小并加速定位。证据：Stage 04 `FilePartitionLog.readFrom`。

## E. Stage 05: segmented log

15. segment 文件名使用固定宽度 20 位十进制 base offset 加 `.log`，例如 `00000000000000000042.log`。base offset 让恢复和读取时能知道每个文件覆盖的 partition offset 范围。证据：Stage 05/06 `FilePartitionLog.segmentFileName` 和 `LogSegment.parseBaseOffset`。

16. `append` 先取 active segment，如果 `recordCount() >= maxRecordsPerSegment` 就 `rollSegment()`，然后才写入。写前判断可以保证当前 segment 不超过记录数上限，新记录写入以 `nextOffset` 为 base offset 的新 segment。证据：`FilePartitionLog.append`。

17. 外层 `FilePartitionLog.readFrom` 遍历按 base offset 排序的 `segments`，跳过 `segment.nextOffset() <= offset` 的旧 segment，对可能命中的 segment 调用 `segment.readFrom(offset, remaining)`，直到收满 `maxMessages` 或没有更多 segment。证据：Stage 05/06 `readFrom` 和 `segmentedLogReadsAcrossMultipleSegments` 测试。

18. Segmented log 解决了单个 partition 文件无限增长的问题，并为后续 retention、compaction、index 打基础。它仍没有 per-segment index，所以 segment 内读取仍要从文件头扫描；也没有 retention deletion、checksum、fsync policy、partial write truncation。证据：Stage 05 README 和 `FilePartitionLog` 注释。

## F. Stage 06: consumer groups

19. broker 用 `Map<GroupKey, NavigableSet<String>> groupMembers` 记录成员，`GroupKey` 是 `groupId + topic`。成员集合是 `TreeSet`，目的是让成员顺序稳定，从而 assignment 在测试和阅读时可预测。证据：`labs/mini-kafka/stages/06-consumer-groups/src/main/java/lab/minikafka/broker/AbstractSingleNodeKafkaBroker.java`。

20. 算法是 deterministic round-robin：把 partitions 按 partition number 排序，把 members 按 `TreeSet` 顺序排列，partition index 对 members size 取模。partitions `[0, 1, 2, 3]`、members `[consumer-a, consumer-b]` 时，`consumer-a` 拿 `[0, 2]`，`consumer-b` 拿 `[1, 3]`。证据：`assignmentFor`。

21. local position 在 `MiniKafkaConsumer.positions`，表示该 consumer 下一次要 fetch 的位置；committed offset 在 broker 的 `groupOffsets`，表示 group 已确认处理到哪里。`poll` 会推进 local position，但不会自动写 group committed offset；`commitSync` 才会复制过去。证据：`MiniKafkaConsumer.poll`、`commitSync` 和 `pollStartsAtCommittedOffsetAndCommitStoresLocalPosition` 测试。

22. `poll` 不会立即提交。它只调用 broker `fetch`，然后把本地 position 更新为 `result.nextOffset()`。如果 consumer 在 `commitSync` 前崩溃，broker 里的 committed offset 仍是旧值，新 consumer 会从旧 committed offset 恢复，可能重复读已经 poll 但未提交的消息。证据：同一个 consumer 测试断言 poll 后 committed offset 仍为 `1L`。

23. `commitSync` 先 `refreshAssignment()`，保证只提交当前仍归自己所有的 partitions。这样可以避免 consumer 在 rebalance 后还覆盖已经转移给其他成员的 partition 进度。证据：`MiniKafkaConsumer.commitSync`。

24. `close` 调用 broker `leaveConsumerGroup` 删除成员；剩余 consumer 下次调用 `assignment`、`poll` 或 `commitSync` 时都会通过 `refreshAssignment`/`assignedPartitions` 重新计算 assignment。这里没有 heartbeat、coordinator、rebalance generation、join/sync group 协议，也没有后台自动 rebalance 通知。证据：`MiniKafkaConsumer.close`、`AbstractSingleNodeKafkaBroker.leaveConsumerGroup`、`leavingGroupReassignsPartitionsToRemainingConsumers` 测试。

25. Stage 06 file-backed broker 持久化 record data 和 partition end offset recovery 所需的 segment 文件。topic shape 需要调用者重建，group membership、assignment、local positions、committed offsets 都在内存里。证据：`FileBackedKafkaBroker` 注释、`AbstractSingleNodeKafkaBroker.groupOffsets/groupMembers`，以及 `labs/mini-kafka/notes/scope.md`。

## G. 设计理解与扩展

26. 一个合理的 `07-indexed-segments` 方案是把 `LogSegment` 拆成 package-private class，并新增 segment index 文件，例如 `OffsetIndex` 或 `SegmentIndex`，记录 logical offset 到 file position 的稀疏映射。`append` 写 record 后追加 index entry；`readFrom` 先通过 index 找到不大于目标 offset 的 file position，再从那里扫描少量 record。证据：当前 `LogSegment.readFrom` 注释明确说没有 offset indexes，只能从 segment 开头扫描。

27. 现在 `FetchResult.nextOffset` 是 `requested offset + returned message count`，因为没有 retention gaps。若未来删除旧 segment，requested offset 可能小于 log start offset，或者返回批次可能跨过被删除范围；此时需要引入 log start offset、out-of-range 处理，或者让 nextOffset 基于实际返回的最后一条消息 offset 加一，而不是简单用请求 offset 加数量。证据：Stage 06 `AbstractSingleNodeKafkaBroker.fetch` 里的注释已经提示 retention 会改变该逻辑。

28. 可接受答案示例：Stage 06 `FileBackedKafkaBroker.pathFor` 返回 `partition-<n>.log`，但 segmented `FilePartitionLog` 把传入 path 当目录使用，并在下面创建 `000...log` segment 文件。这个命名容易让读者误以为 `partition-0.log` 是单个文件。可改为 `partition-<n>` 或在测试中明确断言目录布局。其他可接受点包括：committed offset 没有上界校验、partial tail writes 只靠 EOF 停止、assignment 非 sticky、没有更多 malformed segment filename 测试。
