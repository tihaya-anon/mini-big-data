# Mini Big Data Labs

## Positioning

This repository is a curated set of mini labs for learning big data systems by building small, constrained versions of them.

Each lab should satisfy three rules:

1. Keep the implementation small enough to finish.
2. Preserve the core system invariants that make the original design interesting.
3. Prefer clarity of data flow and failure model over feature count.

## Roadmap

| Lab | What to build | Core concepts |
| --- | --- | --- |
| **mini Kafka** | A minimal distributed log with topic partitions, append/read APIs, and consumer group offset tracking. | append-only log, partition, offset, consumer group, replication, leader election |
| **mini LSM KV Store** | A local key-value engine with WAL, memtable flush, immutable SSTables, and background compaction. | MemStore, WAL, SSTable/HFile, compaction, Bloom filter, range scan |
| **mini Spark** | A small batch execution engine that turns transformations into a DAG and executes stages with shuffle boundaries. | DAG, RDD lineage, shuffle, stage split, task scheduler |
| **mini Flink** | A streaming runtime with operators, event-time handling, checkpoints, and state restore. | streaming operator, state backend, checkpoint barrier, watermark, exactly-once |
| **mini Hive Metastore + SQL Planner** | A metadata service plus a toy SQL planner that resolves tables, partitions, and execution plans. | catalog, partition pruning, logical plan, physical plan |
| **mini Iceberg** | A table format prototype with snapshots, manifests, metadata files, and schema evolution. | snapshot, manifest, metadata.json, time travel, schema evolution |
| **mini Terraform** | An infrastructure engine prototype that parses config, builds a dependency graph, and computes plan/apply steps. | HCL parser, state, diff/plan/apply, provider interface, dependency graph |

## Suggested structure for each lab

Each lab directory follows the same shape:

```text
lab-name/
├── README.md
├── notes/
│   └── scope.md
├── src/
└── tests/
```

Use that structure to keep design notes, implementation, and tests separate from the start.

## Recommended build order

1. `mini-lsm-kv-store`
2. `mini-kafka`
3. `mini-spark`
4. `mini-flink`
5. `mini-hive-metastore-sql-planner`
6. `mini-iceberg`
7. `mini-terraform`

This order starts with local storage and execution basics, then moves toward metadata and orchestration layers.

## Delivery standard

Each lab should eventually include:

1. A short problem statement.
2. A scoped feature list with explicit non-goals.
3. A minimal architecture diagram or data flow description.
4. A working implementation under `src/`.
5. Runnable checks under `tests/`.
6. A short retrospective on tradeoffs and missing pieces.

## Next step

Use the template in [templates/lab/README.md](../templates/lab/README.md) when you start filling in an individual lab.
