# Stage 03: consumer offsets

Track consumer progress separately from message storage.

## Pain point

Fetching from an offset works, but the broker does not yet remember where each consumer group has
processed up to. A restart or handoff needs a committed position to resume from.

## What this stage adds

- committed offsets per consumer group
- committed offsets per topic partition
- independent progress for different groups over the same log

## What this stage leaves out

- consumer group membership
- partition assignment across group members
- disk persistence

## Commands

- `cd labs/mini-kafka/stages/03-consumer-offsets && mvn test`

