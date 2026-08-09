# CognoDB Graph Database Benchmark

A comparative benchmark of **CognoDB** against managed/cloud graph databases using the same dataset, workload patterns, and benchmark methodology.

## Overview

This project evaluates graph database ingestion and query performance across:

* **CognoDB**
* **FalkorDB Cloud**
* **Memgraph Cloud**
* **Neo4j Aura**
* **ArangoDB Cloud**

The goal is to compare:

1. Dataset ingestion performance
2. Graph traversal latency
3. Point and indexed lookups
4. Aggregation performance
5. Concurrent mixed read/write workloads
6. Overall throughput

---

## Dataset

The same dataset was loaded into every database.

| Metric                |                  Value |
| --------------------- | ---------------------: |
| Nodes                 |                169,924 |
| Relationships         |                100,000 |
| Node label/collection |               `Person` |
| Relationship type     |         `CONNECTED_TO` |
| Node identifier       |            `Person.id` |
| Batch size            | 5,000 for most loaders |
| Read/write workload   |              80% / 20% |

The dataset is loaded from:

```text
data/
├── nodes.csv
└── relationships.csv
```

---

## Project Structure

```text
cogno/
│
├── data/
│   ├── nodes.csv
│   └── relationships.csv
│
├── loader/
│   ├── load_cognoDB.py
│   ├── load_falkordb.py
│   ├── load_memgraph.py
│   ├── load_neo4jaura.py
│   └── load_arangodb.py
│
├── benchmark/
│   ├── cognoDB_benchmark.py
│   ├── falkorDB_benchmark.py
│   ├── memgraph_benchmark.py
│   ├── neo4jaura_benchmark.py
│   └── arangoDB_benchmark.py
│
├── results/
│   └── ingest/
│       ├── ingest_cognodb.json
│       ├── ingest_falkordb.json
│       ├── ingest_memgraph.json
│       ├── ingest_neo4jaura.json
│       └── ingest_arangodb.json
│
├── results_cognodb.json
├── results_falkordb.json
├── results_memgraph.json
├── results/results_neo4j_aura.json
├── results_arangodb.json
│
├── requirements.txt
├── .env.example
└── README.md
```

---

# Benchmark Methodology

Each database was populated with the same:

* **169,924 nodes**
* **100,000 relationships**

The benchmark includes:

### Traversals

* 1-hop traversal
* 2-hop traversal
* 3-hop traversal

### Lookups

* Point lookup
* Indexed/filtered lookup

### Aggregation

* Total relationship count

### Mixed workload

A concurrent workload containing approximately:

* 80% reads
* 20% writes

Concurrency levels:

```text
1
10
40
```

For the cloud benchmarks, each workload uses:

```text
100 iterations
10 warmup iterations
```

Latency is reported using:

* **p50** — median latency
* **p95** — 95th percentile latency

---

# Ingestion Results

| Database   |     Nodes/sec | Relationships/sec |       Overall ingest rate |
| ---------- | ------------: | ----------------: | ------------------------: |
| Neo4j Aura | **20,589.82** |      **7,642.50** | **12,650.20 records/sec** |
| CognoDB    |      6,604.57 |          4,017.95 |      5,332.72 records/sec |
| FalkorDB   |      5,991.49 |          1,242.78 |      2,480.33 records/sec |
| ArangoDB   |      4,703.03 |          4,882.67 |      4,768.02 records/sec |
| Memgraph   |      2,913.11 |          3,506.25 |      3,107.89 records/sec |

### Ingestion observations

Neo4j Aura achieved the highest ingestion throughput in this run, followed by CognoDB.

CognoDB achieved:

```text
6,604.57 nodes/sec
4,017.95 relationships/sec
5,332.72 records/sec overall
```

The complete dataset was successfully loaded and verified.

---

# Read Benchmark Results

## 1-Hop Traversal

| Database   |          p50 |          p95 |
| ---------- | -----------: | -----------: |
| FalkorDB   | **56.20 ms** | **80.93 ms** |
| Neo4j Aura |    101.12 ms |    119.68 ms |
| Memgraph   |    300.08 ms |    429.90 ms |
| CognoDB    |    320.58 ms |    469.52 ms |
| ArangoDB   |    321.09 ms |    525.17 ms |

## 2-Hop Traversal

| Database   |          p50 |          p95 |
| ---------- | -----------: | -----------: |
| FalkorDB   | **56.34 ms** | **81.06 ms** |
| Neo4j Aura |    100.58 ms |    124.27 ms |
| Memgraph   |    320.03 ms |    438.80 ms |
| CognoDB    |    325.35 ms |    959.01 ms |
| ArangoDB   |    326.25 ms |    588.36 ms |

## 3-Hop Traversal

| Database   |          p50 |          p95 |
| ---------- | -----------: | -----------: |
| FalkorDB   | **52.77 ms** | **72.66 ms** |
| Neo4j Aura |    101.28 ms |    119.08 ms |
| Memgraph   |    320.02 ms |    355.41 ms |
| CognoDB    |    324.28 ms |    936.69 ms |
| ArangoDB   |    321.08 ms |    492.39 ms |

---

# Lookup Results

| Database   | Point lookup p50 | Point lookup p95 | Filtered lookup p50 | Filtered lookup p95 |
| ---------- | ---------------: | ---------------: | ------------------: | ------------------: |
| FalkorDB   |     **53.07 ms** |     **82.31 ms** |        **57.05 ms** |        **81.61 ms** |
| Neo4j Aura |        100.89 ms |        114.18 ms |           101.47 ms |           116.50 ms |
| Memgraph   |        317.22 ms |        439.65 ms |           319.43 ms |           372.03 ms |
| CognoDB    |        326.18 ms |        939.78 ms |           320.87 ms |           542.16 ms |
| ArangoDB   |        319.98 ms |        390.51 ms |           320.77 ms |           449.02 ms |

---

# Aggregation Results

| Database   |          p50 |          p95 |
| ---------- | -----------: | -----------: |
| FalkorDB   | **58.28 ms** | **73.17 ms** |
| Neo4j Aura |     99.65 ms |    115.03 ms |
| Memgraph   |    351.21 ms |    599.23 ms |
| CognoDB    |    315.21 ms |    391.25 ms |
| ArangoDB   |    328.01 ms |    521.76 ms |

---

# Mixed Read/Write Results

## CognoDB

```text
Clients:              10
Operations:           500
Reads:                404
Writes:               96
Throughput:           16.91 ops/sec
p50 latency:          309.32 ms
p95 latency:          1077.46 ms
```

## Neo4j Aura

```text
Clients:              10
Operations:           500
Reads:                403
Writes:               97
Throughput:           90.97 ops/sec
p50 latency:          89.54 ms
p95 latency:          193.40 ms
```

## FalkorDB Cloud

| Concurrency |         Throughput |
| ----------: | -----------------: |
|           1 |      17.68 ops/sec |
|          10 |     154.60 ops/sec |
|          40 | **442.43 ops/sec** |

Read latency remained around:

```text
p50: ~53–57 ms
p95: ~73–82 ms
```

## Memgraph Cloud

| Concurrency |        Throughput |
| ----------: | ----------------: |
|           1 |      2.19 ops/sec |
|          10 |     19.91 ops/sec |
|          40 | **85.73 ops/sec** |

## ArangoDB Cloud

| Concurrency |        Throughput |
| ----------: | ----------------: |
|           1 |      2.46 ops/sec |
|          10 |     24.90 ops/sec |
|          40 | **97.33 ops/sec** |

---

# Summary

Based on this benchmark run:

### Fastest read performance

**FalkorDB Cloud** showed the lowest traversal and lookup latency.

Its traversal p50 remained approximately:

```text
53–57 ms
```

across the tested workloads.

### Strongest mixed-workload scalability

FalkorDB reached:

```text
442.43 operations/sec
```

at concurrency 40 in the tested workload.

### Strongest ingestion performance

**Neo4j Aura** achieved the highest measured ingestion rate:

```text
12,650.20 records/sec
```

### CognoDB

CognoDB successfully handled the complete dataset:

```text
169,924 nodes
100,000 relationships
```

and achieved:

```text
6,604.57 nodes/sec
4,017.95 relationships/sec
5,332.72 records/sec overall
```

Its mixed workload achieved:

```text
16.91 ops/sec
309.32 ms p50
1077.46 ms p95
```

These results provide a baseline for further CognoDB optimization.

---

# Ingestion Verification

Every database successfully reached the expected dataset size.

```text
Expected nodes         : 169,924
Expected relationships : 100,000
```

Final verified counts:

```text
CognoDB       : 169,924 nodes / 100,000 relationships
FalkorDB      : 169,924 nodes / 100,000 relationships
Memgraph      : 169,924 nodes / 100,000 relationships
Neo4j Aura    : 169,924 nodes / 100,000 relationships
ArangoDB      : 169,924 nodes / 100,000 relationships
```

---

# Important Benchmark Caveats

The benchmark is intended as an **engineering comparison under the tested environment**, not a universal ranking of graph databases.

Cloud performance depends on:

* Cloud tier
* CPU allocation
* Memory allocation
* Network latency
* Region
* Database configuration
* Connection overhead
* Query implementation
* Index configuration

Therefore, the reported numbers should be interpreted as measurements from this specific test environment.

### FalkorDB

The mixed workload inserts synthetic nodes/edges. The database should be reset using the loader before repeating the benchmark.

### Memgraph

The mixed workload creates temporary benchmark records and deletes them within the same transaction.

### Neo4j Aura

The loader reported that the `Person.id` constraint could not be created because a corresponding index already existed:

```text
Neo.ClientError.Schema.IndexAlreadyExists
```

The existing `Person.id` index was already present, and the dataset and benchmark completed successfully.

### Cloud resources

Exact CPU/RAM specifications were not included where they were not observable from the benchmark output. Cloud-tier resource differences should therefore be considered when interpreting the comparison.

---

# Running the Project

## 1. Install dependencies

```bash
pip install -r requirements.txt
```

## 2. Configure environment variables

Create a `.env` file:

```env
# CognoDB
COGNODB_URI=...

# FalkorDB
FALKORDB_HOST=...
FALKORDB_PORT=...
FALKORDB_USERNAME=...
FALKORDB_PASSWORD=...

# Memgraph
MEMGRAPH_URI=...
MEMGRAPH_USERNAME=...
MEMGRAPH_PASSWORD=...

# Neo4j Aura
NEO4J_AURA_URI=...
NEO4J_AURA_USERNAME=...
NEO4J_AURA_PASSWORD=...

# ArangoDB
ARANGODB_PASSWORD=...
encodedCA=...
```

Do **not** commit `.env` or credentials to GitHub.

---

# Loading the Databases

### CognoDB

```bash
python loader/load_cognoDB.py
```

### FalkorDB

```bash
python loader/load_falkordb.py
```

### Memgraph

```bash
python loader/load_memgraph.py
```

### Neo4j Aura

```bash
python loader/load_neo4jaura.py
```

### ArangoDB

```bash
python loader/load_arangodb.py
```

---

# Running Benchmarks

### CognoDB

```bash
python benchmark/cognoDB_benchmark.py
```

### FalkorDB

```bash
python benchmark/falkorDB_benchmark.py
```

### Memgraph

```bash
python benchmark/memgraph_benchmark.py
```

### Neo4j Aura

```bash
python benchmark/neo4jaura_benchmark.py
```

### ArangoDB

```bash
python benchmark/arangoDB_benchmark.py
```

---

# Results

Benchmark results are stored as JSON files so the measurements can be inspected or processed programmatically.

```text
results/
├── ingest/
│   ├── ingest_cognodb.json
│   ├── ingest_falkordb.json
│   ├── ingest_memgraph.json
│   ├── ingest_neo4jaura.json
│   └── ingest_arangodb.json
│
├── results_cognodb.json
├── results_neo4j_aura.json
└── ...
```

---

# Technologies

* Python
* Pandas
* tqdm
* Neo4j Python Driver
* ArangoDB Python Driver
* FalkorDB
* Memgraph
* Cypher
* AQL
* REST/HTTP APIs
* Cloud-hosted graph databases

---

# Conclusion

This benchmark provides a reproducible comparison of five graph database platforms using an identical dataset and workload structure.

The results demonstrate that database performance varies significantly depending on the workload:

* **FalkorDB** performed particularly well for low-latency graph reads and concurrent workloads.
* **Neo4j Aura** demonstrated strong ingestion and read performance.
* **ArangoDB** provided consistent graph operations with moderate concurrent throughput.
* **Memgraph** successfully handled the dataset but showed higher query latency in this environment.
* **CognoDB** successfully ingested and queried the full dataset and provides a baseline for continued performance optimization.

The benchmark results can be used to identify optimization opportunities in CognoDB and to guide future improvements in indexing, query execution, batching, connection handling, and concurrent workload processing.
