# mini Kafka

Build a Kafka-like log in small runnable stages. Each stage keeps one version of the code so you can
start from the core log idea and watch the design get more complex only when a new pain point needs
to be solved.

See [notes/scope.md](notes/scope.md) for the staged roadmap and non-goals.

## Why Kafka Exists

Kafka is a durable event log for moving data between systems.

Instead of one service calling another service directly every time something happens, a producer
writes an event into a shared log. Consumers read that log at their own pace. This gives you:

- one write that can feed many downstream systems
- replay, because old events stay in the log
- decoupling, because producers do not need to wait for every consumer
- ordering within a partition

If you only remember one sentence, remember this: Kafka is mostly an append-only log with a
disciplined way to divide, read, persist, and track progress through that log.

## Stages

| Stage | Code | Main idea | New pain point solved |
| --- | --- | --- | --- |
| 01 | [stages/01-append-log](stages/01-append-log) | one append-only log | replay records from a known offset |
| 02 | [stages/02-topic-partitions](stages/02-topic-partitions) | topic partitions | split one stream into independent ordered logs |
| 03 | [stages/03-consumer-offsets](stages/03-consumer-offsets) | committed offsets | remember each group's progress separately from records |
| 04 | [stages/04-file-backed-log](stages/04-file-backed-log) | file-backed logs | keep records across broker recreation |
| 05 | [stages/05-segmented-log](stages/05-segmented-log) | segment files | avoid one endlessly growing partition file |
| 06 | [stages/06-consumer-groups](stages/06-consumer-groups) | consumer groups | split partitions across consumers in one group |

## Core Concepts

### Topic

A topic is a named stream of events, such as `orders` or `payments`.

### Partition

A topic is split into partitions. Each partition is its own ordered log. Kafka uses partitions to
scale writes and reads horizontally.

Ordering is guaranteed inside one partition, not across the whole topic.

### Offset

Every message in a partition gets an offset: `0`, `1`, `2`, and so on.

An offset is not a random ID. It is the position of a message in that partition's log. Consumers use
offsets to say where to start reading, what has already been processed, and where to resume after a
restart.

### Segment

A segment is one chunk of a partition log on disk. Real Kafka does not keep one partition forever in
one giant file; it rolls to new segment files so recovery, retention, deletion, and indexing stay
bounded.

### Consumer Group

Multiple consumers can share one group ID. Conceptually, the group is cooperating to consume topic
partitions together, and the group stores its progress as committed offsets.

Stage 06 implements a teaching-sized version of this: consumers join a group for one topic, the
broker assigns partitions with deterministic round-robin assignment, each consumer tracks local
positions, and `commitSync` writes those positions as committed group offsets.

Stage 06 still does not implement Kafka's full rebalance protocol, durable group offset storage, or
multi-topic subscriptions.

## How to Read This Lab

Read one stage at a time:

1. Open the stage README.
2. Read the tests first to see the behavior.
3. Read the smallest implementation class that makes those tests pass.
4. Move to the next stage and ask what new complexity appeared.

This structure intentionally repeats code across stages. The repetition keeps each version runnable
and lets you compare before and after states without checking out git history.

## Structure

```text
mini-kafka/
├── README.md
├── pom.xml
├── notes/
│   └── scope.md
└── stages/
    ├── 01-append-log/
    ├── 02-topic-partitions/
    ├── 03-consumer-offsets/
    ├── 04-file-backed-log/
    ├── 05-segmented-log/
    └── 06-consumer-groups/
```

Each stage has its own `pom.xml`, `README.md`, `src/main/java`, and `src/test/java`.

## Commands

- `cd labs/mini-kafka && mvn test`: run every stage
- `cd labs/mini-kafka/stages/01-append-log && mvn test`: run one stage
- `cd labs/mini-kafka && mvn -pl stages/06-consumer-groups test`: run one stage from the aggregator
- `cd labs/mini-kafka && mvn fmt:format`: apply google-java-format to all stage sources
- `cd labs/mini-kafka && mvn fmt:check`: check all stage sources

## What Comes Next

The next useful stage is `07-indexed-segments`: add per-segment indexes so fetch does not need to
scan segment files sequentially.
