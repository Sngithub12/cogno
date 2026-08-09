import os
import json
import time
import random
import threading

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

# --------------------------------------------------
# Connection
# --------------------------------------------------

HOST = os.getenv("MEMGRAPH_HOST")
PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
USERNAME = os.getenv("MEMGRAPH_USERNAME", "")
PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")

URI = f"bolt+ssc://{HOST}:{PORT}"

GRAPH_NAME = "cogno_benchmark"

# --------------------------------------------------
# Benchmark configuration
# --------------------------------------------------

N_ITERATIONS = 100
N_WARMUP = 10

CONCURRENCY_LEVELS = [1, 10, 40]

MIXED_DURATION_SECONDS = 15
READ_WRITE_RATIO = 0.80

EXPECTED_NODES = 169924
EXPECTED_RELATIONSHIPS = 100000

PLATFORM_NAME = "Memgraph Cloud"
RESULTS_PATH = "results_memgraph.json"


# --------------------------------------------------
# Connection
# --------------------------------------------------

def create_driver():
    return GraphDatabase.driver(
        URI,
        auth=(USERNAME, PASSWORD)
    )


# --------------------------------------------------
# Percentiles
# --------------------------------------------------

def percentiles(values, pcts=(50, 95)):
    values = sorted(values)

    result = {}

    for p in pcts:
        index = min(
            len(values) - 1,
            int((p / 100) * len(values))
        )

        result[f"p{p}"] = round(values[index], 3)

    return result


# --------------------------------------------------
# Query helper
# --------------------------------------------------

def run_query(session, query, params=None):
    result = session.run(
        query,
        params or {}
    )

    return result


def time_query(driver, query, params_fn, iterations, warmup):

    # Warmup
    with driver.session() as session:
        for _ in range(warmup):
            result = session.run(
                query,
                params_fn()
            )

            result.consume()

    latencies = []

    # Measured runs
    with driver.session() as session:

        for _ in range(iterations):

            params = params_fn()

            start = time.perf_counter()

            result = session.run(
                query,
                params
            )

            result.consume()

            elapsed = (
                time.perf_counter() - start
            ) * 1000

            latencies.append(elapsed)

    return latencies


# --------------------------------------------------
# Dataset verification
# --------------------------------------------------

def verify_dataset(driver):

    print("\nVerifying dataset...")

    with driver.session() as session:

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

    print(f"Nodes: {node_count}")
    print(f"Relationships: {relationship_count}")

    if node_count != EXPECTED_NODES:
        raise RuntimeError(
            f"Expected {EXPECTED_NODES} nodes, "
            f"found {node_count}"
        )

    if relationship_count != EXPECTED_RELATIONSHIPS:
        raise RuntimeError(
            f"Expected {EXPECTED_RELATIONSHIPS} relationships, "
            f"found {relationship_count}"
        )


# --------------------------------------------------
# Sample valid IDs
# --------------------------------------------------

def get_sample_ids(driver, n=200):

    print("\nSampling start nodes...")

    with driver.session() as session:

        result = session.run(
            """
            MATCH (n:Person)
            RETURN n.id AS id
            LIMIT $limit
            """,
            limit=n
        )

        ids = [
            record["id"]
            for record in result
        ]

    print(f"Sampled {len(ids)} node ids")

    if not ids:
        raise RuntimeError(
            "No Person nodes found."
        )

    return ids


# --------------------------------------------------
# 1. Traversals
# --------------------------------------------------

def bench_traversals(driver, sample_ids):

    print("\n[1/5] Traversals...")

    results = {}

    queries = {

        "1_hop": """
            MATCH (a:Person {id: $id})
                  -[:CONNECTED_TO]->(b)
            RETURN count(b) AS count
        """,

        "2_hop": """
            MATCH (a:Person {id: $id})
                  -[:CONNECTED_TO*2]->(b)
            RETURN count(b) AS count
        """,

        "3_hop": """
            MATCH (a:Person {id: $id})
                  -[:CONNECTED_TO*3]->(b)
            RETURN count(b) AS count
        """
    }

    for name, query in queries.items():

        print(f"Running {name} traversal...")

        ids = iter(
            sample_ids *
            (
                (
                    N_ITERATIONS + N_WARMUP
                ) // len(sample_ids) + 1
            )
        )

        def params_fn(ids=ids):
            return {
                "id": next(ids)
            }

        latencies = time_query(
            driver,
            query,
            params_fn,
            N_ITERATIONS,
            N_WARMUP
        )

        results[name] = {
            **percentiles(latencies),
            "unit": "ms"
        }

    return results


# --------------------------------------------------
# 2. Lookups
# --------------------------------------------------

def bench_lookups(driver, sample_ids):

    print("\n[2/5] Lookups...")

    results = {}

    # ----------------------------------------------
    # Point lookup
    # ----------------------------------------------

    print("Running point lookup...")

    ids = iter(
        sample_ids *
        (
            (
                N_ITERATIONS + N_WARMUP
            ) // len(sample_ids) + 1
        )
    )

    point_query = """
        MATCH (n:Person {id: $id})
        RETURN n.id AS id
    """

    latencies = time_query(
        driver,
        point_query,
        lambda: {
            "id": next(ids)
        },
        N_ITERATIONS,
        N_WARMUP
    )

    results["point_lookup"] = {
        **percentiles(latencies),
        "unit": "ms",
        "indexed_property": "Person.id"
    }

    # ----------------------------------------------
    # Filtered lookup
    # ----------------------------------------------

    print("Running indexed/filtered lookup...")

    ids2 = iter(
        sample_ids *
        (
            (
                N_ITERATIONS + N_WARMUP
            ) // len(sample_ids) + 1
        )
    )

    filtered_query = """
        MATCH (n:Person)
        WHERE n.id > $id
        RETURN n.id AS id
        LIMIT 50
    """

    latencies = time_query(
        driver,
        filtered_query,
        lambda: {
            "id": next(ids2)
        },
        N_ITERATIONS,
        N_WARMUP
    )

    results["filtered_lookup"] = {
        **percentiles(latencies),
        "unit": "ms"
    }

    return results


# --------------------------------------------------
# 3. Aggregation
# --------------------------------------------------

def bench_aggregations(driver):

    print("\n[3/5] Aggregations...")

    print(
        "Running aggregation "
        "(total relationship count)..."
    )

    query = """
        MATCH ()-[r:CONNECTED_TO]->()
        RETURN count(r) AS total_relationships
    """

    latencies = time_query(
        driver,
        query,
        lambda: {},
        N_ITERATIONS,
        N_WARMUP
    )

    return {
        "total_relationship_count": {
            **percentiles(latencies),
            "unit": "ms"
        }
    }


# --------------------------------------------------
# 4. Mixed workload
# --------------------------------------------------

def mixed_worker(
    stop_at,
    sample_ids,
    counters,
    lock,
    client_id
):

    driver = create_driver()

    local_reads = 0
    local_writes = 0
    local_latencies = []

    random.seed(
        1000 + client_id
    )

    try:

        with driver.session() as session:

            while time.perf_counter() < stop_at:

                start = time.perf_counter()

                if random.random() < READ_WRITE_RATIO:

                    node_id = random.choice(
                        sample_ids
                    )

                    result = session.run(
                        """
                        MATCH (
                            a:Person {id: $id}
                        )-[:CONNECTED_TO]->(b)

                        RETURN count(b) AS count
                        """,
                        id=node_id
                    )

                    result.consume()

                    local_reads += 1

                else:

                    benchmark_id = (
                        f"bench_{client_id}_"
                        f"{time.time_ns()}"
                    )

                    # Temporary write.
                    # Create and delete in one
                    # transaction so the benchmark
                    # does not permanently change
                    # the dataset.

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

                    local_writes += 1

                elapsed = (
                    time.perf_counter()
                    - start
                ) * 1000

                local_latencies.append(
                    elapsed
                )

    finally:
        driver.close()

    with lock:

        counters["reads"] += local_reads

        counters["writes"] += local_writes

        counters["latencies"].extend(
            local_latencies
        )


def bench_mixed_workload(sample_ids):

    print(
        "\n[4/5] Mixed concurrent workload..."
    )

    results = {}

    for concurrency in CONCURRENCY_LEVELS:

        print(
            f"Running mixed workload "
            f"at concurrency={concurrency}..."
        )

        counters = {
            "reads": 0,
            "writes": 0,
            "latencies": []
        }

        lock = threading.Lock()

        stop_at = (
            time.perf_counter()
            + MIXED_DURATION_SECONDS
        )

        threads = []

        start = time.perf_counter()

        for client_id in range(concurrency):

            thread = threading.Thread(
                target=mixed_worker,
                args=(
                    stop_at,
                    sample_ids,
                    counters,
                    lock,
                    client_id
                )
            )

            threads.append(thread)

            thread.start()

        for thread in threads:
            thread.join()

        elapsed = (
            time.perf_counter()
            - start
        )

        total_ops = (
            counters["reads"]
            + counters["writes"]
        )

        latencies = counters["latencies"]

        result = {
            "elapsed_seconds": round(
                elapsed,
                2
            ),

            "total_ops": total_ops,

            "reads": counters["reads"],

            "writes": counters["writes"],

            "read_write_ratio_target":
                READ_WRITE_RATIO,

            "throughput_ops_per_sec":
                round(
                    total_ops / elapsed,
                    2
                )
        }

        if latencies:

            result.update({
                "p50_latency_ms":
                    round(
                        percentiles(
                            latencies
                        )["p50"],
                        3
                    ),

                "p95_latency_ms":
                    round(
                        percentiles(
                            latencies
                        )["p95"],
                        3
                    )
            })

        results[
            f"concurrency_{concurrency}"
        ] = result

    return results


# --------------------------------------------------
# 5. Footprint
# --------------------------------------------------

def bench_footprint(driver):

    print("\n[5/5] Footprint...")

    footprint = {
        "dataset": {
            "nodes": EXPECTED_NODES,
            "relationships":
                EXPECTED_RELATIONSHIPS
        },

        "notes": [
            "Memgraph Cloud resource limits "
            "depend on the selected Cloud tier.",
            "Check the Memgraph Cloud console "
            "for exact CPU and RAM allocation."
        ]
    }

    return footprint


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    print("#" * 60)
    print("Memgraph Cloud Benchmark")
    print("#" * 60)

    print(f"\nHost: {HOST}")
    print(f"Port: {PORT}")

    print(
        f"Iterations/workload: "
        f"{N_ITERATIONS}"
    )

    print(
        f"Warmup iterations: "
        f"{N_WARMUP}"
    )

    print(
        f"Concurrency levels: "
        f"{CONCURRENCY_LEVELS}"
    )

    print(
        "\nTesting Memgraph Cloud connection..."
    )

    driver = create_driver()

    try:

        with driver.session() as session:

            result = session.run(
                "RETURN 1 AS value"
            ).single()

            print(
                "Connection successful:",
                result["value"]
            )

        verify_dataset(driver)

        sample_ids = get_sample_ids(
            driver,
            n=200
        )

        all_results = {

            "platform":
                PLATFORM_NAME,

            "graph_name":
                GRAPH_NAME,

            "config": {

                "nodes":
                    EXPECTED_NODES,

                "relationships":
                    EXPECTED_RELATIONSHIPS,

                "iterations_per_workload":
                    N_ITERATIONS,

                "warmup_iterations":
                    N_WARMUP,

                "concurrency_levels":
                    CONCURRENCY_LEVELS,

                "mixed_workload_duration_seconds":
                    MIXED_DURATION_SECONDS,

                "read_write_ratio":
                    READ_WRITE_RATIO
            },

            "caveats": [
                "Mixed workload creates temporary "
                "BenchmarkNode records and deletes "
                "them in the same transaction.",
                "Benchmark uses Neo4j Python driver "
                "with bolt+ssc TLS connection."
            ]
        }

        all_results["traversals"] = (
            bench_traversals(
                driver,
                sample_ids
            )
        )

        all_results["lookups"] = (
            bench_lookups(
                driver,
                sample_ids
            )
        )

        all_results["aggregations"] = (
            bench_aggregations(
                driver
            )
        )

        all_results["mixed_workload"] = (
            bench_mixed_workload(
                sample_ids
            )
        )

        all_results["footprint"] = (
            bench_footprint(
                driver
            )
        )

    finally:

        driver.close()

    with open(
        RESULTS_PATH,
        "w"
    ) as f:

        json.dump(
            all_results,
            f,
            indent=2
        )

    print(
        f"\nResults saved to: "
        f"{RESULTS_PATH}"
    )

    print("\nResults:")

    print(
        json.dumps(
            all_results,
            indent=2
        )
    )


if __name__ == "__main__":
    main()