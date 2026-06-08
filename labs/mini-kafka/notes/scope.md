# mini Kafka Scope

## Goal

Understand Kafka from the inside out by starting with the smallest useful model: a partitioned append-only log plus consumer progress tracking.

This lab is for readers who may know that Kafka is "a message queue" or "an event streaming system" but do not yet understand the storage model underneath those descriptions.

## First milestone

Implement a single-node version before adding replication or leader election.

## In scope now

- create a topic with a fixed number of partitions
- append messages to a partition and return offsets
- fetch messages from a partition starting at an offset
- track committed offsets per consumer group
- join consumers to a single-topic group
- assign partitions across group members with a simple deterministic strategy
- let consumers poll from local positions and commit progress
- persist partition logs to disk
- recover partition end offsets after restart
- roll to new segment files as the log grows

## Persistence boundary

The file-backed broker persists record data only. On restart, callers recreate the expected topic
shape with `createTopic`, and each partition log recovers its end offset by scanning segment files.

Consumer group membership, assignments, and offsets remain in memory for this milestone. Persisting
them would require a separate offset log and recovery path, which is intentionally left for a later
coordination-focused phase.

## Out of scope now

- per-segment index files
- replication and leader failover
- production-grade consumer group rebalance protocol
- multi-topic consumer subscriptions
- durable consumer group offset storage
- socket protocol or HTTP API

## Concepts to internalize in this phase

- Kafka is fundamentally a log, not a request-response RPC system.
- A partition is the unit of ordering.
- Offsets are positions in the log, not arbitrary message IDs.
- Consumer progress is separate from message storage.
- A consumer's local position and a group's committed offset are different pieces of state.
- Persistence changes implementation details, but not the append/fetch contract.
- Segments make an ever-growing log operationally manageable.

## Next milestone

Add per-segment indexes so fetch no longer depends on scanning segment files sequentially.

The immediate follow-up should include:

- an index file per segment
- clearer active versus closed segment roles
- tail recovery rules for incomplete writes
