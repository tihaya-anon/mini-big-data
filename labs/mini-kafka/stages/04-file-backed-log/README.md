# Stage 04: file-backed log

Persist partition logs to disk so records survive broker object recreation.

## Pain point

The in-memory broker loses records when the process exits. Kafka is useful because the log is
durable, so this stage moves partition records behind a storage abstraction and adds a file-backed
implementation.

## What this stage adds

- a `PartitionLogStore` abstraction
- in-memory and file-backed partition logs
- shared broker flow for both storage implementations
- restart recovery of partition end offsets

## What this stage leaves out

- segment rollover
- index files
- durable consumer group offsets
- consumer group membership

## Read the code

- `src/main/java/lab/minikafka/storage/PartitionLogStore.java`: storage abstraction boundary
- `src/main/java/lab/minikafka/storage/FilePartitionLog.java`: file format and restart recovery
- `src/main/java/lab/minikafka/broker/AbstractSingleNodeKafkaBroker.java`: shared broker flow
- `src/test/java/lab/minikafka/broker/FileBackedKafkaBrokerTest.java`: persistence behavior

## Commands

- `cd labs/mini-kafka/stages/04-file-backed-log && mvn test`
