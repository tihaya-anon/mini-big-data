# Stage 01: append log

Start with the smallest useful Kafka idea: one ordered append-only log.

## Pain point

A producer needs somewhere to write events, and a consumer needs to replay events from a known
position. There are no topics, partitions, consumer groups, persistence, or networking yet.

## What this stage adds

- records with offsets
- append that returns the assigned offset
- fetch from an offset with a maximum batch size
- end offset lookup

## What this stage leaves out

- topic names
- multiple partitions
- consumer progress tracking
- disk persistence

## Read the code

- `src/main/java/lab/minikafka/PartitionLog.java`: append, fetch, and end-offset behavior
- `src/main/java/lab/minikafka/Message.java`: immutable record payload with an offset
- `src/test/java/lab/minikafka/PartitionLogTest.java`: the smallest behavior contract

## Commands

- `cd labs/mini-kafka/stages/01-append-log && mvn test`
