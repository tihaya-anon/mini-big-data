# Stage 06: consumer groups

Add a simple single-topic consumer group model.

## Pain point

Committed offsets say where a group has processed up to, but they do not coordinate which consumer
owns each partition. A group needs membership and assignment so multiple consumers can share work
without reading the same partition at the same time.

## What this stage adds

- single-topic group membership
- deterministic round-robin partition assignment
- a small `MiniKafkaConsumer` with local positions
- `commitSync` to copy local positions into group committed offsets

## What this stage leaves out

- multi-topic subscriptions
- heartbeat and rebalance protocols
- durable group membership and committed offsets
- replication and leader election

## Read the code

- `src/main/java/lab/minikafka/broker/AbstractSingleNodeKafkaBroker.java`: group membership and assignment
- `src/main/java/lab/minikafka/consumer/MiniKafkaConsumer.java`: local position and commit behavior
- `src/main/java/lab/minikafka/storage/FilePartitionLog.java`: latest segmented storage model
- `src/test/java/lab/minikafka/consumer/MiniKafkaConsumerTest.java`: group assignment and polling

## Commands

- `cd labs/mini-kafka/stages/06-consumer-groups && mvn test`
