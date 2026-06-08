# Stage 05: segmented log

Split each file-backed partition log into bounded segment files.

## Pain point

A single ever-growing file is hard to recover, delete, index, and operate. Real Kafka rolls partition
logs into segment files so old ranges can be managed independently.

## What this stage adds

- fixed-size segment rollover
- segment file naming by base offset
- reads across multiple segments
- restart recovery across segment files

## What this stage leaves out

- per-segment indexes
- retention and deletion
- durable consumer group offsets
- consumer group membership

## Commands

- `cd labs/mini-kafka/stages/05-segmented-log && mvn test`

