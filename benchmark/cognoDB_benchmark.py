
# benchmark/cognodb_full_benchmark.py

from neo4j import GraphDatabase
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import random
import time
import statistics
import json
import traceback

load_dotenv()

# ============================================================
# Configuration
# ============================================================

URI = os.getenv("COGNODB_URI")
USERNAME = os.getenv("COGNODB_USERNAME")
PASSWORD = os.getenv("COGNODB_PASSWORD")

if not URI or not USERNAME or not PASSWORD:
    raise ValueError(
        "Missing CognoDB credentials. Check your .env file."
    )

GRAPH_NAME = "cogno_benchmark"

# Read benchmark
ITERATIONS = 100
WARMUP = 10
TOTAL_NODES = 169924

# Mixed workload
CLIENTS = 10
OPERATIONS_PER_CLIENT = 50
READ_RATIO = 0.80

OUTPUT_FILE = "results/results_cognodb.json"

random.seed(42)


# ============================================================
# Utility functions
# ============================================================

def percentile(values, p):
    values = sorted(values)

    index = int(
        (p / 100) * (len(values) - 1)
    )

    return values[index]


def calculate_metrics(latencies):
    return {
        "p50_ms": round(percentile(latencies, 50), 3),
        "p95_ms": round(percentile(latencies, 95), 3),
        "min_ms": round(min(latencies), 3),
        "max_ms": round(max(latencies), 3),
        "mean_ms": round(statistics.mean(latencies), 3),
    }


# ============================================================
# 1. Standard Read Benchmark
# ============================================================

def run_parameterized_benchmark(
    session,
    workload_name,
    query,
    start_nodes
):
    print(f"\nRunning {workload_name}...")

    # ----------------------------
    # Warm-up
    # ----------------------------

    for node_id in start_nodes[:WARMUP]:
        session.run(
            query,
            node_id=node_id
        ).consume()

    # ----------------------------
    # Measurement
    # ----------------------------

    latencies = []

    for node_id in start_nodes[WARMUP:]:

        start = time.perf_counter()

        session.run(
            query,
            node_id=node_id
        ).consume()

        end = time.perf_counter()

        latencies.append(
            (end - start) * 1000
        )

    metrics = calculate_metrics(latencies)

    print(
        f"p50={metrics['p50_ms']:.2f} ms | "
        f"p95={metrics['p95_ms']:.2f} ms | "
        f"mean={metrics['mean_ms']:.2f} ms"
    )

    return {
        "workload": workload_name,
        **metrics
    }


# ============================================================
# Aggregation Benchmark
# ============================================================

def run_aggregation_benchmark(session):

    print("\nRunning aggregation...")

    query = """
        MATCH ()-[r:CONNECTED_TO]->()
        RETURN count(r) AS total_relationships
    """

    # Warm-up
    for _ in range(WARMUP):
        session.run(query).consume()

    latencies = []

    # Measurement
    for _ in range(ITERATIONS):

        start = time.perf_counter()

        session.run(query).consume()

        end = time.perf_counter()

        latencies.append(
            (end - start) * 1000
        )

    metrics = calculate_metrics(latencies)

    print(
        f"p50={metrics['p50_ms']:.2f} ms | "
        f"p95={metrics['p95_ms']:.2f} ms | "
        f"mean={metrics['mean_ms']:.2f} ms"
    )

    return {
        "workload": "aggregation",
        **metrics
    }


# ============================================================
# Read Benchmark
# ============================================================

def run_read_benchmark(driver):

    print("\n" + "=" * 60)
    print("CognoDB Read Benchmark")
    print("=" * 60)

    start_nodes = random.sample(
        range(TOTAL_NODES),
        ITERATIONS + WARMUP
    )

    queries = {

        "1-hop": """
            MATCH (p:Person {id: $node_id})
            MATCH (p)-[:CONNECTED_TO]->(n)
            RETURN count(n) AS count
        """,

        "2-hop": """
            MATCH (p:Person {id: $node_id})
            MATCH (p)-[:CONNECTED_TO*2]->(n)
            RETURN count(n) AS count
        """,

        "3-hop": """
            MATCH (p:Person {id: $node_id})
            MATCH (p)-[:CONNECTED_TO*3]->(n)
            RETURN count(n) AS count
        """,

        "point_lookup": """
            MATCH (p:Person {id: $node_id})
            RETURN p.id
        """,

        "filtered_lookup": """
            MATCH (p:Person)
            WHERE p.id = $node_id
            RETURN p.id
        """
    }

    results = []

    with driver.session() as session:

        for workload_name, query in queries.items():

            result = run_parameterized_benchmark(
                session,
                workload_name,
                query,
                start_nodes
            )

            results.append(result)

        aggregation_result = run_aggregation_benchmark(
            session
        )

        results.append(
            aggregation_result
        )

    return results


# ============================================================
# Mixed Read/Write Worker
# ============================================================

def mixed_worker(client_id):

    driver = None

    try:

        driver = GraphDatabase.driver(
            URI,
            auth=(USERNAME, PASSWORD)
        )

        reads = 0
        writes = 0
        latencies = []

        random.seed(42 + client_id)

        with driver.session() as session:

            for operation in range(
                OPERATIONS_PER_CLIENT
            ):

                node_id = random.randint(
                    0,
                    TOTAL_NODES - 1
                )

                start = time.perf_counter()

                # ----------------------------
                # READ
                # ----------------------------

                if random.random() < READ_RATIO:

                    session.run(
                        """
                        MATCH (p:Person {id: $node_id})
                        RETURN p.id
                        """,
                        node_id=node_id
                    ).consume()

                    reads += 1

                # ----------------------------
                # WRITE
                # ----------------------------

                else:

                    benchmark_id = (
                        f"bench_{client_id}_{operation}"
                    )

                    # Create and delete inside
                    # one transaction.
                    def create_and_delete(tx):

                        tx.run(
                            """
                            CREATE (
                                n:BenchmarkNode {
                                    id: $id
                                }
                            )
                            WITH n
                            DELETE n
                            """,
                            id=benchmark_id
                        ).consume()

                    session.execute_write(
                        create_and_delete
                    )

                    writes += 1

                end = time.perf_counter()

                latencies.append(
                    (end - start) * 1000
                )

        return {
            "client": client_id,
            "reads": reads,
            "writes": writes,
            "latencies": latencies
        }

    except Exception as e:

        print(
            f"\nERROR in client {client_id}: "
            f"{type(e).__name__}: {e}"
        )

        traceback.print_exc()

        raise

    finally:

        if driver:
            driver.close()


# ============================================================
# Mixed Read/Write Benchmark
# ============================================================

def run_mixed_benchmark():

    print("\n" + "=" * 60)
    print("CognoDB Mixed Read/Write Benchmark")
    print("=" * 60)

    print(f"Clients: {CLIENTS}")
    print(
        f"Operations/client: "
        f"{OPERATIONS_PER_CLIENT}"
    )

    print(
        f"Read/Write mix: "
        f"{int(READ_RATIO * 100)}% / "
        f"{int((1 - READ_RATIO) * 100)}%"
    )

    total_expected = (
        CLIENTS * OPERATIONS_PER_CLIENT
    )

    print(
        f"Total operations: {total_expected}"
    )

    print("\nStarting mixed benchmark...\n")

    start_time = time.perf_counter()

    results = []

    with ThreadPoolExecutor(
        max_workers=CLIENTS
    ) as executor:

        futures = [
            executor.submit(
                mixed_worker,
                client_id
            )
            for client_id in range(CLIENTS)
        ]

        for future in as_completed(futures):

            result = future.result()

            results.append(result)

            print(
                f"Client {result['client']} completed: "
                f"{result['reads']} reads, "
                f"{result['writes']} writes"
            )

    elapsed = (
        time.perf_counter() - start_time
    )

    # ----------------------------
    # Combine results
    # ----------------------------

    total_reads = sum(
        result["reads"]
        for result in results
    )

    total_writes = sum(
        result["writes"]
        for result in results
    )

    total_operations = (
        total_reads + total_writes
    )

    all_latencies = []

    for result in results:
        all_latencies.extend(
            result["latencies"]
        )

    throughput = (
        total_operations / elapsed
    )

    metrics = calculate_metrics(
        all_latencies
    )

    mixed_result = {
        "clients": CLIENTS,
        "operations_per_client":
            OPERATIONS_PER_CLIENT,
        "total_operations":
            total_operations,
        "reads":
            total_reads,
        "writes":
            total_writes,
        "read_write_ratio_target":
            READ_RATIO,
        "elapsed_seconds":
            round(elapsed, 3),
        "throughput_ops_per_sec":
            round(throughput, 3),
        "p50_latency_ms":
            metrics["p50_ms"],
        "p95_latency_ms":
            metrics["p95_ms"],
        "min_latency_ms":
            metrics["min_ms"],
        "max_latency_ms":
            metrics["max_ms"],
        "mean_latency_ms":
            metrics["mean_ms"]
    }

    print("\n" + "=" * 60)
    print("Mixed Benchmark Results")
    print("=" * 60)

    print(
        f"Total operations : "
        f"{total_operations}"
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
        f"{metrics['p50_ms']:.2f} ms"
    )

    print(
        f"p95 latency      : "
        f"{metrics['p95_ms']:.2f} ms"
    )

    print("=" * 60)

    return mixed_result


# ============================================================
# Main
# ============================================================

def main():

    print("\n" + "#" * 60)
    print("CognoDB Full Benchmark")
    print("#" * 60)

    print("\nTesting CognoDB connection...")

    driver = GraphDatabase.driver(
        URI,
        auth=(USERNAME, PASSWORD)
    )

    try:

        # ----------------------------
        # Connection test
        # ----------------------------

        with driver.session() as session:

            result = session.run(
                "RETURN 1 AS value"
            ).single()

            print(
                f"Connection successful: "
                f"{result['value']}"
            )

            # ----------------------------
            # Dataset verification
            # ----------------------------

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

        # ----------------------------
        # Read benchmark
        # ----------------------------

        print("\n[1/2] Running read workloads...")

        read_results = run_read_benchmark(
            driver
        )

        # ----------------------------
        # Mixed benchmark
        # ----------------------------

        print(
            "\n[2/2] Running mixed "
            "read/write workload..."
        )

        mixed_results = run_mixed_benchmark()

        # ----------------------------
        # Build final result
        # ----------------------------

        final_results = {

            "platform": "CognoDB",

            "graph_name": GRAPH_NAME,

            "dataset": {
                "nodes": node_count,
                "relationships":
                    relationship_count
            },

            "read_benchmark": {
                "iterations":
                    ITERATIONS,
                "warmup":
                    WARMUP,
                "results":
                    read_results
            },

            "mixed_workload": mixed_results,

            "caveats": [
                "Mixed workload creates temporary BenchmarkNode records and deletes them in the same transaction.",
                "Read benchmark uses 100 measurement iterations after 10 warm-up iterations.",
                "Mixed benchmark uses 10 concurrent clients with 50 operations per client and an 80/20 read/write target."
            ]
        }

        # ----------------------------
        # Save JSON
        # ----------------------------

        os.makedirs(
            "results",
            exist_ok=True
        )

        with open(
            OUTPUT_FILE,
            "w"
        ) as file:

            json.dump(
                final_results,
                file,
                indent=2
            )

        # ----------------------------
        # Final summary
        # ----------------------------

        print("\n" + "=" * 60)
        print("CognoDB Full Benchmark Complete")
        print("=" * 60)

        print("\nRead workloads:")

        for result in read_results:

            print(
                f"{result['workload']:20}"
                f"p50={result['p50_ms']:8.2f} ms | "
                f"p95={result['p95_ms']:8.2f} ms"
            )

        print("\nMixed workload:")

        print(
            f"Throughput: "
            f"{mixed_results['throughput_ops_per_sec']:.2f} ops/sec"
        )

        print(
            f"p50 latency: "
            f"{mixed_results['p50_latency_ms']:.2f} ms"
        )

        print(
            f"p95 latency: "
            f"{mixed_results['p95_latency_ms']:.2f} ms"
        )

        print("=" * 60)

        print(
            f"\nResults saved to: "
            f"{OUTPUT_FILE}"
        )

    finally:

        driver.close()


if __name__ == "__main__":
    main()

