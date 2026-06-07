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

This lab currently implements a single-node in-memory broker with:

- topic creation with fixed partition count
- append that returns a monotonically increasing partition offset
- fetch from an offset with a max message count
- consumer group offset commit and lookup

Replication, leader election, rebalancing, persistence, and network protocols are intentionally out of scope for this phase.

## Mapping this lab to real Kafka

| Real Kafka idea | This lab |
| --- | --- |
| broker | `InMemoryKafkaBroker` |
| topic-partition log | `PartitionLog` |
| record with offset | `Message` |
| fetch response | `FetchResult` |
| committed group offset | `groupOffsets` map inside the broker |

This version is deliberately in-memory so the basic data model is easy to see. Real Kafka adds disk-backed segments, replication, leader election, retention, compaction, network protocols, and consumer group coordination on top of the same underlying ideas.

## How to read this lab

If you are new to Kafka, read in this order:

1. `README.md`: understand the system model
2. `notes/scope.md`: understand what this phase includes and excludes
3. `src/main/java/lab/minikafka/PartitionLog.java`: see the append-only log directly
4. `src/main/java/lab/minikafka/InMemoryKafkaBroker.java`: see how topic-partitions and group offsets are managed
5. `src/test/java/lab/minikafka/InMemoryKafkaBrokerTest.java`: see the expected behavior end to end

## Structure

```text
mini-kafka/
├── pom.xml
├── notes/
├── src/main/java/lab/minikafka/
└── src/test/java/lab/minikafka/
```

## Commands

- `cd labs/mini-kafka && mvn test`: compile and run the current tests
- `cd labs/mini-kafka && mvn package`: build the lab artifact

## What comes next

The next milestone should move the partition log from memory to disk-backed segments. That is the point where this lab starts to look much more like a real message log instead of just an in-memory data structure.
