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

### Consumer group

Multiple consumers can share one group ID. Conceptually, the group is cooperating to consume a topic together, and the group stores its progress as committed offsets.

This lab only implements the offset tracking part. It does not yet implement partition assignment or rebalance.

## What this lab is trying to teach

This lab strips Kafka down to the minimum ideas that matter first:

1. Writing to a partition is just appending to a log.
2. Reading is asking for messages starting from an offset.
3. A consumer group mostly needs progress tracking before it needs coordination.
4. Once these ideas are clear, persistence, replication, and failover are easier to reason about.

## Current milestone

This lab currently implements two single-node broker variants:

- an in-memory broker for learning the pure data model first
- a file-backed broker for learning persistence and restart behavior

Across those two variants, the lab now covers:

- topic creation with fixed partition count
- append that returns a monotonically increasing partition offset
- fetch from an offset with a max message count
- consumer group offset commit and lookup
- file-backed append-only partition logs
- restart recovery of partition end offsets

Replication, leader election, rebalancing, segmented storage, retention, and network protocols are still out of scope for this phase.

## Mapping this lab to real Kafka

| Real Kafka idea | This lab |
| --- | --- |
| broker | `InMemoryKafkaBroker`, `FileBackedKafkaBroker` |
| topic-partition log abstraction | `PartitionLogStore` |
| in-memory log | `InMemoryPartitionLog` |
| file-backed log | `FilePartitionLog` |
| record with offset | `Message` |
| fetch response | `FetchResult` |
| shared single-node broker flow | `AbstractSingleNodeKafkaBroker` |
| committed group offset | `groupOffsets` map inside the abstract broker |

This lab now has both a pure in-memory model and a minimal persistent model. Real Kafka still goes further by adding segmented logs, indexes, replication, leader election, retention, compaction, network protocols, and consumer group coordination.

## How to read this lab

If you are new to Kafka, read in this order:

1. `README.md`: understand the system model
2. `notes/scope.md`: understand what this phase includes and excludes
3. `src/main/java/lab/minikafka/storage/InMemoryPartitionLog.java`: see the append-only log in its simplest form
4. `src/main/java/lab/minikafka/storage/FilePartitionLog.java`: see how persistence changes the design
5. `src/main/java/lab/minikafka/broker/AbstractSingleNodeKafkaBroker.java`: see the shared broker flow
6. `src/test/java/lab/minikafka/broker/InMemoryKafkaBrokerTest.java`: see the memory behavior
7. `src/test/java/lab/minikafka/broker/FileBackedKafkaBrokerTest.java`: see restart and persistence behavior

## Structure

```text
mini-kafka/
├── pom.xml
├── notes/
├── src/main/java/lab/minikafka/
│   ├── api/
│   ├── broker/
│   ├── model/
│   └── storage/
└── src/test/java/lab/minikafka/broker/
```

## Commands

- `cd labs/mini-kafka && mvn test`: compile and run the current tests
- `cd labs/mini-kafka && mvn package`: build the lab artifact

## What comes next

The next milestone should move from a single log file per partition to segmented storage with explicit segment boundaries and, later, indexes. That is the point where this lab starts to resemble Kafka's real on-disk log design much more closely.
