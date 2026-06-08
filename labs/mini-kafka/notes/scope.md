# mini Kafka Scope

## Goal

Understand Kafka from the inside out by building increasingly capable versions of the same small
single-node system.

The lab is organized as runnable stages. Each stage solves one new pain point and intentionally
keeps earlier limitations visible.

## Stage Roadmap

| Stage | In scope | Out of scope |
| --- | --- | --- |
| `01-append-log` | one in-memory append-only log, offsets, fetch batches | topics, partitions, consumer progress, persistence |
| `02-topic-partitions` | topic creation, fixed partitions, independent partition offsets | committed offsets, consumer groups, persistence |
| `03-consumer-offsets` | committed offsets per group and topic partition | group membership, assignment, durable offsets |
| `04-file-backed-log` | file-backed partition logs, restart end-offset recovery | segment rollover, indexes, durable offsets |
| `05-segmented-log` | bounded segment files, rollover, reads across segments | indexes, retention, compaction |
| `06-consumer-groups` | single-topic group membership, deterministic partition assignment, local consumer positions | multi-topic subscriptions, production rebalance protocol, durable group state |

## Current Latest Stage

`stages/06-consumer-groups` is the latest teaching version. It has both in-memory and segmented
file-backed broker variants, plus a small consumer API.

The latest stage covers:

- topic creation with fixed partition count
- append that returns a monotonically increasing partition offset
- fetch from an offset with a max message count
- committed consumer group offset lookup and commit
- segmented file-backed append-only partition logs
- restart recovery of partition end offsets
- single-topic group membership and partition assignment
- local consumer positions and synchronous offset commit

## Persistence Boundary

The file-backed broker persists record data only. On restart, callers recreate the expected topic
shape with `createTopic`, and each partition log recovers its end offset by scanning segment files.

Consumer group membership, assignments, and offsets remain in memory. Persisting them would require
a separate offset log and recovery path, similar in spirit to Kafka's `__consumer_offsets` topic.

## Concepts To Internalize

- Kafka is fundamentally a log, not a request-response RPC system.
- A partition is the unit of ordering.
- Offsets are positions in the log, not arbitrary message IDs.
- Consumer progress is separate from message storage.
- A consumer's local position and a group's committed offset are different pieces of state.
- Persistence changes implementation details, but not the append/fetch contract.
- Segments make an ever-growing log operationally manageable.

## Repository Shape

The lab root is a Maven aggregator. Every stage is an independent Maven module under
`labs/mini-kafka/stages/`.

This is a teaching choice: code is repeated so each stage stays readable, runnable, and directly
comparable to the next stage.

## Next Stage

Add per-segment indexes so fetch no longer depends on scanning segment files sequentially.

The immediate follow-up should include:

- an index file per segment
- clearer active versus closed segment roles
- tail recovery rules for incomplete writes
