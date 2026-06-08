# mini Kafka

Build a minimal Kafka-like log to understand what Kafka is really doing underneath the API.

See [notes/scope.md](notes/scope.md) before implementation.

## Why Kafka exists

Kafka is a durable event log for moving data between systems.

Instead of one service calling another service directly every time something happens, a producer writes an event into a shared log. Consumers then read that log at their own pace. This gives you:

- one write that can feed many downstream systems
- replay, because old events stay in the log
- decoupling, because producers do not need to wait for every consumer
- ordering within a partition

If you only remember one sentence, remember this: Kafka is mostly an append-only log with a disciplined way to divide, read, and track progress.

## Core ideas in plain language

### Topic

A topic is a named stream of events, such as `orders` or `payments`.

### Partition

A topic is split into partitions. Each partition is its own ordered log. Kafka uses partitions to scale writes and reads horizontally.

Ordering is guaranteed inside one partition, not across the whole topic.

### Offset

Every message in a partition gets an offset: `0`, `1`, `2`, and so on.

An offset is not a random ID. It is the position of a message in that partition's log. Consumers use offsets to say:

- where to start reading
- what has already been processed
- where to resume after a restart

### Segment

A segment is one chunk of a partition log on disk.

Real Kafka does not keep one partition forever in one giant file. Instead, it splits the log into multiple ordered files called segments. New writes go to the active segment, and once that segment grows large enough, Kafka rolls to a new one.

Segmentation solves several practical problems:

- recovery is easier because the broker only needs to inspect bounded files
- retention and deletion are easier because old segments can be removed as units
- indexes stay manageable because they are typically built per segment
- one endlessly growing file is harder to operate than many ordered chunks

### Consumer group

Multiple consumers can share one group ID. Conceptually, the group is cooperating to consume a topic together, and the group stores its progress as committed offsets.

This lab implements the first single-node version of that idea:

- the broker tracks committed offsets per group and topic partition
- consumers join a group for one topic
- the broker assigns topic partitions to group members with deterministic round-robin assignment
- each consumer keeps a local position for its assigned partitions
- `commitSync` writes the consumer's local positions back as committed group offsets

It does not implement Kafka's full rebalance protocol, durable consumer group offset storage, or multi-topic subscriptions.

## What this lab is trying to teach

This lab strips Kafka down to the minimum ideas that matter first:

1. Writing to a partition is just appending to a log.
2. Reading is asking for messages starting from an offset.
3. A consumer group mostly needs progress tracking before it needs coordination.
4. Once these ideas are clear, persistence, replication, and failover are easier to reason about.

## Current milestone

This lab currently implements two single-node broker variants:

- an in-memory broker for learning the pure data model first
- a segmented file-backed broker for learning persistence and restart behavior

Across those two variants, the lab now covers:

- topic creation with fixed partition count
- append that returns a monotonically increasing partition offset
- fetch from an offset with a max message count
- consumer group offset commit and lookup
- simple consumer group membership and partition assignment
- a consumer API that polls from local positions and commits progress
- segmented file-backed append-only partition logs
- restart recovery of partition end offsets
- rollover from one segment file to the next

The file-backed broker persists records and reconstructs each partition's end offset from segment
files. It does not yet persist topic metadata, consumer group membership, or committed consumer group
offsets; tests recreate the topic definition before reading recovered records.

Replication, leader election, the production rebalance protocol, retention policies, index files, and network protocols are still out of scope for this phase.

## Mapping this lab to real Kafka

| Real Kafka idea | This lab |
| --- | --- |
| broker | `InMemoryKafkaBroker`, `FileBackedKafkaBroker` |
| topic-partition log abstraction | `PartitionLogStore` |
| in-memory log | `InMemoryPartitionLog` |
| segmented file-backed log | `FilePartitionLog` |
| record with offset | `Message` |
| fetch response | `FetchResult` |
| consumer with local positions | `MiniKafkaConsumer` |
| shared single-node broker flow | `AbstractSingleNodeKafkaBroker` |
| committed group offset | `groupOffsets` map inside the abstract broker |
| group membership and assignment | `groupMembers` map inside the abstract broker |

This lab now has both a pure in-memory model and a minimal persistent segmented model. Real Kafka still goes further by adding indexes, replication, leader election, retention, compaction, network protocols, and full consumer group coordination.

## How to read this lab

If you are new to Kafka, read in this order:

1. `README.md`: understand the system model
2. `notes/scope.md`: understand what this phase includes and excludes
3. `src/main/java/lab/minikafka/storage/InMemoryPartitionLog.java`: see the append-only log in its simplest form
4. `src/main/java/lab/minikafka/storage/FilePartitionLog.java`: see how persistence and segmentation change the design
5. `src/main/java/lab/minikafka/broker/AbstractSingleNodeKafkaBroker.java`: see the shared broker flow
6. `src/main/java/lab/minikafka/consumer/MiniKafkaConsumer.java`: see local position tracking and commits
7. `src/test/java/lab/minikafka/broker/InMemoryKafkaBrokerTest.java`: see the memory behavior
8. `src/test/java/lab/minikafka/consumer/MiniKafkaConsumerTest.java`: see group assignment and polling
9. `src/test/java/lab/minikafka/broker/FileBackedKafkaBrokerTest.java`: see restart, rollover, and segmented reads

## Structure

```text
mini-kafka/
├── pom.xml
├── notes/
├── src/main/java/lab/minikafka/
│   ├── api/
│   ├── broker/
│   ├── consumer/
│   ├── model/
│   └── storage/
└── src/test/java/lab/minikafka/
    ├── broker/
    ├── consumer/
    └── model/
```

## Commands

- `cd labs/mini-kafka && mvn test`: compile and run the current tests
- `cd labs/mini-kafka && mvn package`: build the lab artifact
- `cd labs/mini-kafka && mvn fmt:format`: apply google-java-format to Java sources
- `cd labs/mini-kafka && mvn fmt:check`: check Java sources against google-java-format

## What comes next

The next milestone should add per-segment indexes and more explicit recovery behavior.

Concretely, the next step should do three things:

1. add a small index per segment so fetch does not need to scan every record sequentially
2. separate active-segment writes from read-only closed segments more explicitly
3. define recovery rules for partially written tail records

That is the point where reads stop being "scan files in order" and start to resemble Kafka's real on-disk log design more closely.
