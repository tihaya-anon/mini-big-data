# mini Kafka Scope

## Goal

Understand how a partitioned append-only log exposes ordered reads, offset-based consumption, and leader-based replication.

## First milestone

Implement a single-node version before adding replication or leader election.

## In scope now

- create a topic with a fixed number of partitions
- append messages to a partition and return offsets
- fetch messages from a partition starting at an offset
- track committed offsets per consumer group

## Out of scope now

- disk persistence and segment files
- replication and leader failover
- consumer group rebalance
- socket protocol or HTTP API

## Next milestone

Move from an in-memory log to a persisted segmented log while keeping the same append and fetch contract.
