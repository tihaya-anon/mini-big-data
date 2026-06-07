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
- persist partition logs to disk
- recover partition end offsets after restart

## Out of scope now

- segmented storage and index files
- replication and leader failover
- consumer group rebalance
- socket protocol or HTTP API

## Concepts to internalize in this phase

- Kafka is fundamentally a log, not a request-response RPC system.
- A partition is the unit of ordering.
- Offsets are positions in the log, not arbitrary message IDs.
- Consumer progress is separate from message storage.
- Persistence changes implementation details, but not the append/fetch contract.

## Next milestone

Move from a single file-backed log to segmented storage while keeping the same append and fetch contract.
