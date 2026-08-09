import os
import json
import time
import random
import threading
import statistics
import base64

from dotenv import load_dotenv
from arango import ArangoClient


load_dotenv()


# --------------------------------------------------
# Connection
# --------------------------------------------------

HOST = "https://879864b690d6.arangodb.cloud:18529"
USERNAME = "root"

PASSWORD = os.getenv("ARANGODB_PASSWORD")
ENCODED_CA = os.getenv("encodedCA")

DATABASE = "_system"

NODE_COLLECTION = "Person"
EDGE_COLLECTION = "CONNECTED_TO"


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

PLATFORM_NAME = "ArangoDB Cloud"

RESULTS_PATH = "results_arangodb.json"


# --------------------------------------------------
# Certificate
# --------------------------------------------------

def create_certificate():

    if not ENCODED_CA:
        raise RuntimeError(
            "encodedCA is missing from .env"
        )

    try:

        certificate = base64.b64decode(
            ENCODED_CA
        )

        with open(
            "cert_file.crt",
            "wb"
        ) as f:

            f.write(certificate)

    except Exception as e:

        raise RuntimeError(
            f"Certificate creation failed: {e}"
        )


# --------------------------------------------------
# Connection
# --------------------------------------------------

def create_client():

    return ArangoClient(
        hosts=HOST,
        verify_override="cert_file.crt"
    )


def create_database():

    client = create_client()

    db = client.db(
        DATABASE,
        username=USERNAME,
        password=PASSWORD
    )

    return client, db


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

        result[f"p{p}"] = round(
            values[index],
            3
        )

    return result


# --------------------------------------------------
# Query helper
# --------------------------------------------------

def run_query(db, query, bind_vars=None):

    cursor = db.aql.execute(
        query,
        bind_vars=bind_vars or {}
    )

    return list(cursor)


# --------------------------------------------------
# Time query
# --------------------------------------------------

def time_query(
    db,
    query,
    params_fn,
    iterations,
    warmup
):

    # --------------------------------------------------
    # Warmup
    # --------------------------------------------------

    for _ in range(warmup):

        params = params_fn()

        cursor = db.aql.execute(
            query,
            bind_vars=params
        )

        list(cursor)


    # --------------------------------------------------
    # Measured runs
    # --------------------------------------------------

    latencies = []

    for _ in range(iterations):

        params = params_fn()

        start = time.perf_counter()

        cursor = db.aql.execute(
            query,
            bind_vars=params
        )

        list(cursor)

        elapsed = (
            time.perf_counter()
            - start
        ) * 1000

        latencies.append(elapsed)

    return latencies


# --------------------------------------------------
# Dataset verification
# --------------------------------------------------

def verify_dataset(db):

    print("\nVerifying dataset...")

    node_result = run_query(
        db,
        """
        RETURN LENGTH(
            FOR n IN Person
            RETURN 1
        )
        """
    )

    relationship_result = run_query(
        db,
        """
        RETURN LENGTH(
            FOR r IN CONNECTED_TO
            RETURN 1
        )
        """
    )

    node_count = node_result[0]

    relationship_count = relationship_result[0]

    print(
        f"Nodes: {node_count}"
    )

    print(
        f"Relationships: {relationship_count}"
    )

    if node_count != EXPECTED_NODES:

        raise RuntimeError(
            f"Expected {EXPECTED_NODES} nodes, "
            f"found {node_count}"
        )

    if relationship_count != EXPECTED_RELATIONSHIPS:

        raise RuntimeError(
            f"Expected {EXPECTED_RELATIONSHIPS} "
            f"relationships, found "
            f"{relationship_count}"
        )


# --------------------------------------------------
# Sample valid IDs
# --------------------------------------------------

def get_sample_ids(db, n=200):

    print("\nSampling start nodes...")

    result = run_query(
        db,
        """
        FOR n IN Person
            LIMIT @limit
            RETURN n.id
        """,
        {
            "limit": n
        }
    )

    ids = result

    print(
        f"Sampled {len(ids)} node ids"
    )

    if not ids:

        raise RuntimeError(
            "No Person nodes found."
        )

    return ids


# ==================================================
# 1. TRAVERSALS
# ==================================================

def bench_traversals(
    db,
    sample_ids
):

    print("\n[1/5] Traversals...")

    results = {}

    queries = {

        "1_hop": """
            WITH Person

            FOR v IN 1..1 OUTBOUND
                CONCAT("Person/", @id)
                CONNECTED_TO

            RETURN COUNT(v)
        """,

        "2_hop": """
            WITH Person

            FOR v IN 2..2 OUTBOUND
                CONCAT("Person/", @id)
                CONNECTED_TO

            RETURN COUNT(v)
        """,

        "3_hop": """
            WITH Person

            FOR v IN 3..3 OUTBOUND
                CONCAT("Person/", @id)
                CONNECTED_TO

            RETURN COUNT(v)
        """
    }

    for name, query in queries.items():

        print(
            f"Running {name} traversal..."
        )

        ids = iter(
            sample_ids *
            (
                (
                    N_ITERATIONS
                    + N_WARMUP
                )
                // len(sample_ids)
                + 1
            )
        )

        def params_fn(ids=ids):

            return {
                "id": next(ids)
            }

        latencies = time_query(
            db,
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


# ==================================================
# 2. LOOKUPS
# ==================================================

def bench_lookups(
    db,
    sample_ids
):

    print("\n[2/5] Lookups...")

    results = {}

    # --------------------------------------------------
    # Point lookup
    # --------------------------------------------------

    print("Running point lookup...")

    ids = iter(
        sample_ids *
        (
            (
                N_ITERATIONS
                + N_WARMUP
            )
            // len(sample_ids)
            + 1
        )
    )

    point_query = """

        FOR n IN Person

            FILTER n.id == @id

            RETURN n.id
    """

    latencies = time_query(
        db,
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

    # --------------------------------------------------
    # Filtered lookup
    # --------------------------------------------------

    print(
        "Running indexed/filtered lookup..."
    )

    ids2 = iter(
        sample_ids *
        (
            (
                N_ITERATIONS
                + N_WARMUP
            )
            // len(sample_ids)
            + 1
        )
    )

    filtered_query = """

        FOR n IN Person

            FILTER n.id > @id

            LIMIT 50

            RETURN n.id
    """

    latencies = time_query(
        db,
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


# ==================================================
# 3. AGGREGATION
# ==================================================

def bench_aggregations(db):

    print("\n[3/5] Aggregations...")

    print(
        "Running aggregation "
        "(total relationship count)..."
    )

    query = """

        RETURN LENGTH(
            FOR r IN CONNECTED_TO
            RETURN 1
        )

    """

    latencies = time_query(
        db,
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


# ==================================================
# 4. MIXED WORKLOAD
# ==================================================

def mixed_worker(
    stop_at,
    sample_ids,
    counters,
    lock,
    client_id
):

    client = create_client()

    db = client.db(
        DATABASE,
        username=USERNAME,
        password=PASSWORD
    )

    local_reads = 0
    local_writes = 0
    local_latencies = []

    random.seed(
        1000 + client_id
    )

    try:

        while time.perf_counter() < stop_at:

            start = time.perf_counter()

            # --------------------------------------------------
            # READ
            # --------------------------------------------------

            if (
                random.random()
                < READ_WRITE_RATIO
            ):

                node_id = random.choice(
                    sample_ids
                )

                result = run_query(
                    db,
                    """

                    WITH Person

                    FOR v IN 1..1 OUTBOUND
                        CONCAT(
                            "Person/",
                            @id
                        )
                        CONNECTED_TO

                    RETURN LENGTH(v)

                    """,
                    {
                        "id": node_id
                    }
                )

                local_reads += 1

            # --------------------------------------------------
            # WRITE
            # --------------------------------------------------

            else:

                benchmark_id = (
                    f"bench_{client_id}_"
                    f"{time.time_ns()}"
                )

                # Insert temporary document

                db.collection(
                    NODE_COLLECTION
                ).insert(
                    {
                        "_key":
                            benchmark_id,

                        "id":
                            benchmark_id,

                        "benchmark":
                            True
                    }
                )

                # Immediately delete it

                db.collection(
                    NODE_COLLECTION
                ).delete(
                    benchmark_id
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

        client.close()

    with lock:

        counters["reads"] += (
            local_reads
        )

        counters["writes"] += (
            local_writes
        )

        counters["latencies"].extend(
            local_latencies
        )


def bench_mixed_workload(
    sample_ids
):

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

        for client_id in range(
            concurrency
        ):

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

        latencies = (
            counters["latencies"]
        )

        result = {

            "elapsed_seconds":
                round(
                    elapsed,
                    2
                ),

            "total_ops":
                total_ops,

            "reads":
                counters["reads"],

            "writes":
                counters["writes"],

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


# ==================================================
# 5. FOOTPRINT
# ==================================================

def bench_footprint(db):

    print("\n[5/5] Footprint...")

    return {

        "dataset": {

            "nodes":
                EXPECTED_NODES,

            "relationships":
                EXPECTED_RELATIONSHIPS
        },

        "notes": [

            "ArangoDB Cloud resource "
            "limits depend on the selected "
            "Cloud deployment tier.",

            "Check the ArangoDB Cloud console "
            "for exact CPU and RAM allocation."
        ]
    }


# ==================================================
# MAIN
# ==================================================

def main():

    print("#" * 60)

    print(
        "ArangoDB Cloud Benchmark"
    )

    print("#" * 60)

    print(
        f"\nIterations/workload: "
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
        "\nCreating ArangoDB connection..."
    )

    client, db = create_database()

    try:

        print(
            "Connection successful."
        )

        print(
            "Database:",
            db.name
        )

        print(
            "Version:",
            db.version()
        )

        # --------------------------------------------------
        # Verify
        # --------------------------------------------------

        verify_dataset(db)

        # --------------------------------------------------
        # Sample IDs
        # --------------------------------------------------

        sample_ids = get_sample_ids(
            db,
            n=200
        )

        # --------------------------------------------------
        # Results
        # --------------------------------------------------

        all_results = {

            "platform":
                PLATFORM_NAME,

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
            }
        }

        # --------------------------------------------------
        # 1. Traversals
        # --------------------------------------------------

        all_results["traversals"] = (
            bench_traversals(
                db,
                sample_ids
            )
        )

        # --------------------------------------------------
        # 2. Lookups
        # --------------------------------------------------

        all_results["lookups"] = (
            bench_lookups(
                db,
                sample_ids
            )
        )

        # --------------------------------------------------
        # 3. Aggregations
        # --------------------------------------------------

        all_results["aggregations"] = (
            bench_aggregations(
                db
            )
        )

        # --------------------------------------------------
        # 4. Mixed workload
        # --------------------------------------------------

        all_results["mixed_workload"] = (
            bench_mixed_workload(
                sample_ids
            )
        )

        # --------------------------------------------------
        # 5. Footprint
        # --------------------------------------------------

        all_results["footprint"] = (
            bench_footprint(
                db
            )
        )

    finally:

        client.close()

    # --------------------------------------------------
    # Save results
    # --------------------------------------------------

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

    create_certificate()

    main()