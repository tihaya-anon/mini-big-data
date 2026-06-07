# mini Kafka

Build a minimal Kafka-like log to understand partitioned append, offset-based fetch, and consumer group offset tracking.

See [notes/scope.md](notes/scope.md) before implementation.

## Current milestone

This lab currently implements a single-node in-memory broker with:

- topic creation with fixed partition count
- append that returns a monotonically increasing partition offset
- fetch from an offset with a max message count
- consumer group offset commit and lookup

Replication, leader election, rebalancing, persistence, and network protocols are intentionally out of scope for this phase.

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
