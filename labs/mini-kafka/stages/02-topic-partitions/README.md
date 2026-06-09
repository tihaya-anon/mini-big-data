# Stage 02: topic partitions

Add named topics and fixed partitions around the append-only log.

## Pain point

One log is easy to understand, but it cannot model Kafka's scale and ordering boundary. Kafka needs many independent logs under a topic so producers and consumers can work in parallel.

## What this stage adds

- topic creation with a fixed partition count
- one independent log per topic partition
- append and fetch by `topic + partition`
- independent offsets per partition

## What this stage leaves out

- consumer progress tracking
- consumer groups
- disk persistence

## Read the code

- `src/main/java/lab/minikafka/InMemoryKafkaBroker.java`: topic creation and partition lookup
- `src/main/java/lab/minikafka/TopicPartition.java`: the identity boundary for ordered logs
- `src/test/java/lab/minikafka/InMemoryKafkaBrokerTest.java`: independent offsets per partition

## Commands

- `cd labs/mini-kafka/stages/02-topic-partitions && mvn test`
