# mini Kafka 源码阅读报告模板

这份报告用于检验你是否读懂了 `labs/mini-kafka` 目前已经实现的源码。回答时不要只复述 README；每个问题都应引用至少一个具体源码或测试位置，例如
`labs/mini-kafka/stages/06-consumer-groups/src/main/java/lab/minikafka/consumer/MiniKafkaConsumer.java`。

参考答案在 `labs/mini-kafka/exercises/reference-answers.md`。建议先独立完成本报告，再用参考答案核对。

## 基本信息

- 姓名：
- 日期：
- 阅读到的最新 stage：
- 执行过的测试命令：
- 测试结果摘要：

## 建议阅读顺序

1. `labs/mini-kafka/stages/01-append-log/src/test/java/lab/minikafka/PartitionLogTest.java`
2. `labs/mini-kafka/stages/01-append-log/src/main/java/lab/minikafka/PartitionLog.java`
3. `labs/mini-kafka/stages/02-topic-partitions/src/main/java/lab/minikafka/InMemoryKafkaBroker.java`
4. `labs/mini-kafka/stages/03-consumer-offsets/src/main/java/lab/minikafka/InMemoryKafkaBroker.java`
5. `labs/mini-kafka/stages/04-file-backed-log/src/main/java/lab/minikafka/storage/FilePartitionLog.java`
6. `labs/mini-kafka/stages/05-segmented-log/src/main/java/lab/minikafka/storage/FilePartitionLog.java`
7. `labs/mini-kafka/stages/06-consumer-groups/src/main/java/lab/minikafka/broker/AbstractSingleNodeKafkaBroker.java`
8. `labs/mini-kafka/stages/06-consumer-groups/src/main/java/lab/minikafka/consumer/MiniKafkaConsumer.java`
9. `labs/mini-kafka/stages/06-consumer-groups/src/test/java/lab/minikafka/consumer/MiniKafkaConsumerTest.java`

## 报告问题

### A. Stage 01: append-only log

1. `PartitionLog.append` 如何给新消息分配 offset？为什么第一条消息的 offset 是 `0`？

   源码证据：

   回答：

2. `fetch(offset, maxMessages)` 返回的 `FetchResult.nextOffset` 表示什么？它为什么不是“最后一条消息的 offset”？

   源码证据：

   回答：

3. 如果从等于或大于 `endOffset()` 的 offset 开始 fetch，代码会返回什么？这说明当前 stage 还没有实现哪些真实 Kafka 行为？

   源码证据：

   回答：

### B. Stage 02: topics and partitions

4. Stage 02 如何把一个 topic 拆成多个独立的有序日志？请说明核心数据结构。

   源码证据：

   回答：

5. 为什么同一个 topic 的 partition 0 和 partition 1 都可以各自拥有 offset `0`？

   源码证据：

   回答：

6. 向不存在的 topic partition 写入时会发生什么？这条规则为什么对 broker API 有意义？

   源码证据：

   回答：

### C. Stage 03: committed offsets

7. `commitOffset` 存储的值代表“下一次要读的 offset”还是“最后已处理消息的 offset”？从代码和测试中找证据。

   源码证据：

   回答：

8. 两个 consumer group 读取同一个 topic partition 时，为什么可以有不同的进度？

   源码证据：

   回答：

9. 一个从未提交过 offset 的 group 会从哪里开始读？这个选择对应真实 Kafka 中哪个配置概念？

   源码证据：

   回答：

10. Stage 03 的 committed offset 是否会随消息一起持久化？为什么？

    源码证据：

    回答：

### D. Stage 04: storage abstraction and file-backed log

11. Stage 04 为什么引入 `PartitionLogStore`？它把 broker 和 storage 的哪些职责分开了？

    源码证据：

    回答：

12. `FilePartitionLog` 的磁盘记录格式是什么？请按字段顺序说明。

    源码证据：

    回答：

13. broker 重启后，file-backed log 如何恢复 `endOffset()`？

    源码证据：

    回答：

14. Stage 04 从任意 offset 读取时为什么需要从文件开头扫描？这暴露了下一个应解决的问题是什么？

    源码证据：

    回答：

### E. Stage 05: segmented log

15. Stage 05 的 segment 文件名如何编码 base offset？这个设计解决了什么定位问题？

    源码证据：

    回答：

16. 什么时候会 rollover 到新的 segment？为什么是在写入前判断？

    源码证据：

    回答：

17. `readFrom` 如何跨多个 segment 返回一批消息？

    源码证据：

    回答：

18. Stage 05 的 segmented log 已经解决了哪些问题？仍然没有解决哪些读性能或运维问题？

    源码证据：

    回答：

### F. Stage 06: consumer groups

19. broker 如何记录一个 group 在某个 topic 上有哪些成员？为什么成员集合使用有序结构？

    源码证据：

    回答：

20. 当前 assignment 算法是什么？给定 partitions `[0, 1, 2, 3]` 和 members `[consumer-a, consumer-b]`，两个 consumer 分别会拿到哪些 partition？

    源码证据：

    回答：

21. `MiniKafkaConsumer` 中的 local position 和 broker 中的 committed offset 有什么区别？

    源码证据：

    回答：

22. `poll` 会不会立即提交 offset？如果 consumer poll 到消息后崩溃但没有 `commitSync`，下次 group 会从哪里恢复？

    源码证据：

    回答：

23. `commitSync` 为什么先刷新 assignment，再提交 positions？这避免了什么错误？

    源码证据：

    回答：

24. 一个 consumer close 后，剩余 consumer 如何观察到新的 partition assignment？这里缺少真实 Kafka 的哪些协议机制？

    源码证据：

    回答：

25. Stage 06 的 file-backed broker 持久化了哪些状态？哪些状态仍然只在内存里？

    源码证据：

    回答：

### G. 设计理解与扩展

26. 如果要实现 `07-indexed-segments`，你会先新增哪些文件或类？它们应该和现有 `FilePartitionLog` 如何协作？

    源码证据：

    回答：

27. 这个 mini Kafka 目前没有实现 retention。若未来删除旧 segment，`FetchResult.nextOffset` 的计算可能需要如何改变？

    源码证据：

    回答：

28. 找出一个你认为命名、边界或测试覆盖还可以改进的点，并说明原因。注意不要只说“真实 Kafka 更复杂”，要结合当前源码。

    源码证据：

    回答：

## 自检清单

- 每个回答都引用了具体源码或测试文件。
- 能解释 offset 是“位置”，不是随机 ID。
- 能区分 partition end offset、consumer local position、group committed offset。
- 能说明 in-memory、file-backed、segmented log 的行为差异。
- 能说明 Stage 06 的 group assignment 是教学用简化模型，不是 Kafka 完整 rebalance 协议。
- 能指出至少一个当前实现留下的下一步问题。
