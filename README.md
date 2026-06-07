# Mini Big Data Labs

`mini_big_data` is a collection of small, implementation-oriented labs for core big data infrastructure ideas.

The goal is not to rebuild full production systems. Each lab isolates one system shape, keeps the scope tight, and makes the key tradeoffs visible in code.

## Repository layout

```text
.
├── .mvn/
├── .gitignore
├── README.md
├── docs/
│   └── mini-big-data-labs.md
├── labs/
│   ├── mini-kafka/
│   ├── mini-lsm-kv-store/
│   ├── mini-spark/
│   ├── mini-flink/
│   ├── mini-hive-metastore-sql-planner/
│   ├── mini-iceberg/
│   └── mini-terraform/
└── templates/
    └── lab/
```

## Build conventions

- JVM-based labs use Maven.
- Maven is configured at the repository root in `.mvn/maven.config`.
- Dependencies are cached in repository-local `.m2/repository`, not in `~/.m2`.
- `mini-terraform` is the only Go-based lab.

Common commands:

- `cd labs/mini-kafka && mvn test`
- `cd labs/mini-kafka && mvn package`
- `cd labs/mini-terraform && go test ./...`

## Labs

| Lab | Focus |
| --- | --- |
| `mini-kafka` | Append-only log, partitions, offsets, consumer groups, replication |
| `mini-lsm-kv-store` | WAL, memtable, SSTable layout, compaction, range scan |
| `mini-spark` | DAG execution, shuffle boundaries, stage splitting, task scheduling |
| `mini-flink` | Streaming operators, state, checkpoints, watermarks, exactly-once |
| `mini-hive-metastore-sql-planner` | Catalog, partitions, logical plans, physical plans |
| `mini-iceberg` | Snapshots, manifests, metadata, schema evolution, time travel |
| `mini-terraform` | Config parsing, graph building, diff/plan/apply, provider interface |

See [docs/mini-big-data-labs.md](docs/mini-big-data-labs.md) for the roadmap and scope of each lab.
