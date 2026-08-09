import os
import time
import json
import random
import statistics
from concurrent.futures import ThreadPoolExecutor

from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Configuration
# ============================================================

URI = os.getenv("NEO4J_AURA_URI")
USERNAME = os.getenv("NEO4J_AURA_USERNAME")
PASSWORD = os.getenv("NEO4J_AURA_PASSWORD")

EXPECTED_NODES = 169_924
EXPECTED_RELATIONSHIPS = 100_000

CLIENTS = 10
OPERATIONS_PER_CLIENT = 50

READ_PERCENT = 0.80
WRITE_PERCENT = 0.20

RESULT_FILE = "results/results_neo4j_aura.json"

# Use deterministic IDs from the dataset
RANDOM_SEED = 42

random.seed(RANDOM_SEED)


# ============================================================
# Validation
# ============================================================

if not URI:
    raise ValueError("Missing NEO4J_AURA_URI")

if not USERNAME:
    raise ValueError("Missing NEO4J_AURA_USERNAME")

if not PASSWORD:
    raise ValueError("Missing NEO4J_AURA_PASSWORD")


# ============================================================
# Helpers
# ============================================================

def percentile(values, p):

    if not values:
        return 0

    values = sorted(values)

    index = int(
        (p / 100) * (len(values) - 1)
    )

    return values[index]


def statistics_for(values):

    return {
        "p50_ms": round(
            percentile(values, 50),
            2
        ),
        "p95_ms": round(
            percentile(values, 95),
            2
        ),
        "mean_ms": round(
            statistics.mean(values),
            2
        )
    }


# ============================================================
# Create driver
# ============================================================

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD),
    max_connection_pool_size=20
)


# ============================================================
# Connection / dataset verification
# ============================================================

print("\n" + "#" * 60)
print("Neo4j Aura Full Benchmark")
print("#" * 60)

print("\nTesting Neo4j Aura connection...")

with driver.session() as session:

    result = session.run(
        "RETURN 1 AS value"
    ).single()

    print(
        f"Connection successful: "
        f"{result['value']}"
    )

    print("\nVerifying dataset...")

    node_count = session.run(
        """
        MATCH (n:Person)
        RETURN count(n) AS count
        """
    ).single()["count"]

    relationship_count = session.run(
        """
        MATCH ()-[r:CONNECTED_TO]->()
        RETURN count(r) AS count
        """
    ).single()["count"]

    print(
        f"Nodes: {node_count}"
    )

    print(
        f"Relationships: "
        f"{relationship_count}"
    )

    if node_count != EXPECTED_NODES:
        raise RuntimeError(
            f"Expected {EXPECTED_NODES} nodes, "
            f"got {node_count}"
        )

    if relationship_count != EXPECTED_RELATIONSHIPS:
        raise RuntimeError(
            f"Expected {EXPECTED_RELATIONSHIPS} relationships, "
            f"got {relationship_count}"
        )


# ============================================================
# Read workloads
# ============================================================

READ_QUERIES = {

    "1-hop": """
        MATCH (a:Person {id: $id})
              -[:CONNECTED_TO]->(b)
        RETURN b.id
        LIMIT 10
    """,

    "2-hop": """
        MATCH (a:Person {id: $id})
              -[:CONNECTED_TO]->()
              -[:CONNECTED_TO]->(c)
        RETURN c.id
        LIMIT 10
    """,

    "3-hop": """
        MATCH (a:Person {id: $id})
              -[:CONNECTED_TO]->()
              -[:CONNECTED_TO]->()
              -[:CONNECTED_TO]->(d)
        RETURN d.id
        LIMIT 10
    """,

    "point_lookup": """
        MATCH (n:Person {id: $id})
        RETURN n.id
    """,

    "filtered_lookup": """
        MATCH (n:Person)
        WHERE n.id >= $min_id
          AND n.id <= $max_id
        RETURN n.id
        LIMIT 10
    """,

    "aggregation": """
        MATCH (n:Person)
        RETURN count(n) AS count
    """
}


# ============================================================
# Execute read workload
# ============================================================

def run_read_workload(name, query, iterations=50):

    latencies = []

    with driver.session() as session:

        for _ in range(iterations):

            person_id = random.randint(
                1,
                EXPECTED_NODES
            )

            params = {
                "id": person_id,
                "min_id": 1,
                "max_id": EXPECTED_NODES
            }

            start = time.perf_counter()

            session.run(
                query,
                **params
            ).consume()

            elapsed = (
                time.perf_counter()
                - start
            ) * 1000

            latencies.append(elapsed)

    stats = statistics_for(latencies)

    print(
        f"p50={stats['p50_ms']:.2f} ms | "
        f"p95={stats['p95_ms']:.2f} ms | "
        f"mean={stats['mean_ms']:.2f} ms"
    )

    return stats


# ============================================================
# Read benchmark
# ============================================================

print("\n" + "=" * 60)
print("Neo4j Aura Read Benchmark")
print("=" * 60)

read_results = {}

for name, query in READ_QUERIES.items():

    print(f"\nRunning {name}...")

    read_results[name] = run_read_workload(
        name,
        query
    )


# ============================================================
# Mixed read/write workload
# ============================================================

print("\n" + "=" * 60)
print("Neo4j Aura Mixed Read/Write Benchmark")
print("=" * 60)

print(
    f"Clients: {CLIENTS}"
)

print(
    f"Operations/client: "
    f"{OPERATIONS_PER_CLIENT}"
)

print(
    f"Read/Write mix: "
    f"{int(READ_PERCENT * 100)}% / "
    f"{int(WRITE_PERCENT * 100)}%"
)

total_operations = (
    CLIENTS *
    OPERATIONS_PER_CLIENT
)

print(
    f"Total operations: "
    f"{total_operations}"
)


def mixed_client(client_id):

    reads = 0
    writes = 0

    latencies = []

    with driver.session() as session:

        for _ in range(
            OPERATIONS_PER_CLIENT
        ):

            if random.random() < READ_PERCENT:

                reads += 1

                person_id = random.randint(
                    1,
                    EXPECTED_NODES
                )

                query = """
                    MATCH (n:Person {id: $id})
                    RETURN n.id
                """

                params = {
                    "id": person_id
                }

            else:

                writes += 1

                person_id = random.randint(
                    1,
                    EXPECTED_NODES
                )

                query = """
                    MATCH (n:Person {id: $id})
                    SET n.last_benchmark_write = true
                    RETURN n.id
                """

                params = {
                    "id": person_id
                }

            start = time.perf_counter()

            session.run(
                query,
                **params
            ).consume()

            elapsed = (
                time.perf_counter()
                - start
            ) * 1000

            latencies.append(elapsed)

    print(
        f"Client {client_id} completed: "
        f"{reads} reads, "
        f"{writes} writes"
    )

    return {
        "reads": reads,
        "writes": writes,
        "latencies": latencies
    }


print("\nStarting mixed benchmark...")

start = time.perf_counter()

with ThreadPoolExecutor(
    max_workers=CLIENTS
) as executor:

    results = list(
        executor.map(
            mixed_client,
            range(CLIENTS)
        )
    )

elapsed = (
    time.perf_counter()
    - start
)

all_latencies = []

total_reads = 0
total_writes = 0

for result in results:

    total_reads += result["reads"]
    total_writes += result["writes"]

    all_latencies.extend(
        result["latencies"]
    )


actual_operations = (
    total_reads +
    total_writes
)

throughput = (
    actual_operations /
    elapsed
)

mixed_stats = statistics_for(
    all_latencies
)


# ============================================================
# Mixed results
# ============================================================

print("\n" + "=" * 60)
print("Mixed Benchmark Results")
print("=" * 60)

print(
    f"Total operations : "
    f"{actual_operations}"
)

print(
    f"Reads            : "
    f"{total_reads}"
)

print(
    f"Writes           : "
    f"{total_writes}"
)

print(
    f"Elapsed time     : "
    f"{elapsed:.2f} seconds"
)

print(
    f"Throughput       : "
    f"{throughput:.2f} operations/sec"
)

print(
    f"p50 latency      : "
    f"{mixed_stats['p50_ms']:.2f} ms"
)

print(
    f"p95 latency      : "
    f"{mixed_stats['p95_ms']:.2f} ms"
)

print("=" * 60)


# ============================================================
# Final output
# ============================================================

print("\n" + "=" * 60)
print("Neo4j Aura Full Benchmark Complete")
print("=" * 60)

print("\nRead workloads:")

for name, stats in read_results.items():

    print(
        f"{name:<20} "
        f"p50={stats['p50_ms']:>8.2f} ms | "
        f"p95={stats['p95_ms']:>8.2f} ms"
    )

print("\nMixed workload:")

print(
    f"Throughput: "
    f"{throughput:.2f} ops/sec"
)

print(
    f"p50 latency: "
    f"{mixed_stats['p50_ms']:.2f} ms"
)

print(
    f"p95 latency: "
    f"{mixed_stats['p95_ms']:.2f} ms"
)

print("=" * 60)


# ============================================================
# Save JSON
# ============================================================

output = {

    "database": "Neo4j AuraDB",

    "dataset": {
        "nodes": node_count,
        "relationships": relationship_count
    },

    "configuration": {
        "clients": CLIENTS,
        "operations_per_client": OPERATIONS_PER_CLIENT,
        "read_percent": READ_PERCENT,
        "write_percent": WRITE_PERCENT
    },

    "read_workloads": read_results,

    "mixed_workload": {
        "total_operations": actual_operations,
        "reads": total_reads,
        "writes": total_writes,
        "elapsed_seconds": round(
            elapsed,
            4
        ),
        "throughput_ops_per_sec": round(
            throughput,
            4
        ),
        "p50_ms": mixed_stats["p50_ms"],
        "p95_ms": mixed_stats["p95_ms"],
        "mean_ms": mixed_stats["mean_ms"]
    }
}


os.makedirs(
    "results",
    exist_ok=True
)

with open(
    RESULT_FILE,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        output,
        f,
        indent=2
    )


print(
    f"\nResults saved to: "
    f"{RESULT_FILE}"
)

driver.close()