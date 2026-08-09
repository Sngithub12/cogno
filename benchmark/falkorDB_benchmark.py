import os
import json
import time
import random
import statistics
import threading
from dotenv import load_dotenv
from falkordb import FalkorDB

load_dotenv()

HOST = os.getenv("FALKORDB_HOST")
PORT = int(os.getenv("FALKORDB_PORT", "57891"))
USERNAME = os.getenv("FALKORDB_USERNAME", "falkordb")
PASSWORD = os.getenv("FALKORDB_PASSWORD")

GRAPH_NAME = "cogno_benchmark"

N_ITERATIONS = 100       # per read workload, after warm-up
N_WARMUP = 10
CONCURRENCY_LEVELS = [1, 10, 40]
MIXED_DURATION_SECONDS = 15
READ_WRITE_RATIO = 0.8   # 80% reads, 20% writes in mixed workload

PLATFORM_NAME = "FalkorDB Cloud"
RESULTS_PATH = "results_falkordb.json"


def connect():
    db = FalkorDB(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
    return db.select_graph(GRAPH_NAME)


def percentiles(latencies_ms, pcts=(50, 95)):
    latencies_ms = sorted(latencies_ms)
    out = {}
    for p in pcts:
        idx = min(len(latencies_ms) - 1, int(round((p / 100) * len(latencies_ms))) - 1)
        idx = max(0, idx)
        out[f"p{p}"] = round(latencies_ms[idx], 3)
    return out


def get_sample_ids(graph, n=200):
    result = graph.query(
        """
        MATCH (n:Person)
        RETURN n.id
        ORDER BY rand()
        LIMIT $n
        """,
        {"n": n}
    )
    return [row[0] for row in result.result_set]


def time_query(graph, query, params_fn, iterations, warmup):
    for _ in range(warmup):
        graph.query(query, params_fn())

    latencies = []
    for _ in range(iterations):
        params = params_fn()
        start = time.perf_counter()
        graph.query(query, params)
        latencies.append((time.perf_counter() - start) * 1000)
    return latencies


# --------------------------------------------------
# 1. Traversals (1-hop, 2-hop, 3-hop)
# --------------------------------------------------

def bench_traversals(graph, sample_ids):
    results = {}
    hop_queries = {
        "1_hop": """
            MATCH (a:Person {id: $id})-[:CONNECTED_TO]->(b)
            RETURN count(b)
        """,
        "2_hop": """
            MATCH (a:Person {id: $id})-[:CONNECTED_TO*2]->(b)
            RETURN count(b)
        """,
        "3_hop": """
            MATCH (a:Person {id: $id})-[:CONNECTED_TO*3]->(b)
            RETURN count(b)
        """,
    }

    for label, query in hop_queries.items():
        print(f"  Running {label} traversal...")
        ids_cycle = iter(sample_ids * ((N_ITERATIONS + N_WARMUP) // len(sample_ids) + 1))

        def params_fn(ids_cycle=ids_cycle):
            return {"id": next(ids_cycle)}

        latencies = time_query(graph, query, params_fn, N_ITERATIONS, N_WARMUP)
        results[label] = percentiles(latencies)
        results[label]["unit"] = "ms"

    return results


# --------------------------------------------------
# 2. Lookups (point + indexed/filtered)
# --------------------------------------------------

def bench_lookups(graph, sample_ids):
    results = {}

    print("  Running point lookup...")
    ids_cycle = iter(sample_ids * ((N_ITERATIONS + N_WARMUP) // len(sample_ids) + 1))
    point_query = "MATCH (n:Person {id: $id}) RETURN n"
    latencies = time_query(
        graph, point_query,
        lambda: {"id": next(ids_cycle)},
        N_ITERATIONS, N_WARMUP
    )
    results["point_lookup"] = percentiles(latencies)
    results["point_lookup"]["unit"] = "ms"
    results["point_lookup"]["indexed_property"] = "Person.id"

    print("  Running indexed/filtered lookup...")
    ids_cycle2 = iter(sample_ids * ((N_ITERATIONS + N_WARMUP) // len(sample_ids) + 1))
    filtered_query = "MATCH (n:Person) WHERE n.id > $id RETURN n LIMIT 50"
    latencies2 = time_query(
        graph, filtered_query,
        lambda: {"id": next(ids_cycle2)},
        N_ITERATIONS, N_WARMUP
    )
    results["filtered_lookup"] = percentiles(latencies2)
    results["filtered_lookup"]["unit"] = "ms"

    return results


# --------------------------------------------------
# 3. Aggregations
# --------------------------------------------------

def bench_aggregations(graph):
    print("  Running aggregation (total relationship count)...")
    # Matches the CognoDB reference script's aggregation query exactly
    # (simple count, no group-by sort) for cross-platform comparability,
    # and avoids FalkorDB free-tier query memory limits triggered by
    # materializing + sorting a full degree-count group-by.
    query = """
        MATCH ()-[r:CONNECTED_TO]->()
        RETURN count(r) AS total_relationships
    """
    latencies = time_query(graph, query, lambda: {}, N_ITERATIONS, N_WARMUP)
    return {
        "total_relationship_count": {**percentiles(latencies), "unit": "ms"}
    }


# --------------------------------------------------
# 4. Mixed concurrent read/write workload
# --------------------------------------------------

def mixed_worker(stop_at, sample_ids, counters, lock, next_synthetic_id, id_lock):
    graph = connect()
    local_reads = 0
    local_writes = 0

    while time.perf_counter() < stop_at:
        if random.random() < READ_WRITE_RATIO:
            pid = random.choice(sample_ids)
            graph.query(
                "MATCH (a:Person {id: $id})-[:CONNECTED_TO]->(b) RETURN count(b)",
                {"id": pid}
            )
            local_reads += 1
        else:
            with id_lock:
                new_id = next_synthetic_id[0]
                next_synthetic_id[0] += 1
            src = random.choice(sample_ids)
            graph.query(
                """
                CREATE (n:Person {id: $new_id})
                WITH n
                MATCH (a:Person {id: $src})
                CREATE (a)-[:CONNECTED_TO]->(n)
                """,
                {"new_id": new_id, "src": src}
            )
            local_writes += 1

    with lock:
        counters["reads"] += local_reads
        counters["writes"] += local_writes


def bench_mixed_workload(sample_ids):
    results = {}
    # synthetic ids start well above real dataset range to avoid collisions
    next_synthetic_id = [10_000_000]
    id_lock = threading.Lock()

    for concurrency in CONCURRENCY_LEVELS:
        print(f"  Running mixed workload at concurrency={concurrency}...")
        counters = {"reads": 0, "writes": 0}
        lock = threading.Lock()
        stop_at = time.perf_counter() + MIXED_DURATION_SECONDS

        threads = [
            threading.Thread(
                target=mixed_worker,
                args=(stop_at, sample_ids, counters, lock, next_synthetic_id, id_lock)
            )
            for _ in range(concurrency)
        ]

        start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - start

        total_ops = counters["reads"] + counters["writes"]
        results[f"concurrency_{concurrency}"] = {
            "elapsed_seconds": round(elapsed, 2),
            "total_ops": total_ops,
            "reads": counters["reads"],
            "writes": counters["writes"],
            "read_write_ratio_target": READ_WRITE_RATIO,
            "throughput_ops_per_sec": round(total_ops / elapsed, 2),
        }

    return results


# --------------------------------------------------
# 5. Footprint
# --------------------------------------------------

def bench_footprint(graph):
    footprint = {
        "advertised_specs": {
            "vcpu": "burstable, not fixed (check FalkorDB Cloud free-tier docs)",
            "ram_mb": "please confirm in FalkorDB Cloud console for your tier",
            "disk_gb": "please confirm in FalkorDB Cloud console for your tier",
            "note": "Fill in exact advertised specs from your FalkorDB Cloud dashboard to match README fairness table."
        }
    }

    try:
        mem_result = graph.query("CALL dbms.showMemoryStats() YIELD * RETURN *")
        footprint["memory_stats"] = mem_result.result_set
    except Exception:
        footprint["memory_stats"] = "not observable via Cypher on this FalkorDB tier"

    try:
        info = graph.execute_command("GRAPH.MEMORY", "USAGE", GRAPH_NAME) if hasattr(graph, "execute_command") else None
        footprint["graph_memory_usage"] = info if info else "not observable"
    except Exception:
        footprint["graph_memory_usage"] = "not observable (GRAPH.MEMORY USAGE not supported by client/tier)"

    return footprint


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    print(f"Connecting to {PLATFORM_NAME}...")
    graph = connect()

    print("Sampling start nodes...")
    sample_ids = get_sample_ids(graph, n=200)
    print(f"  Sampled {len(sample_ids)} node ids")

    all_results = {
        "platform": PLATFORM_NAME,
        "graph_name": GRAPH_NAME,
        "config": {
            "iterations_per_workload": N_ITERATIONS,
            "warmup_iterations": N_WARMUP,
            "concurrency_levels": CONCURRENCY_LEVELS,
            "mixed_workload_duration_seconds": MIXED_DURATION_SECONDS,
            "read_write_ratio": READ_WRITE_RATIO,
        },
        "caveats": [],
    }

    print("\n[1/5] Traversals...")
    all_results["traversals"] = bench_traversals(graph, sample_ids)

    print("\n[2/5] Lookups...")
    all_results["lookups"] = bench_lookups(graph, sample_ids)

    print("\n[3/5] Aggregations...")
    all_results["aggregations"] = bench_aggregations(graph)

    print("\n[4/5] Mixed concurrent workload...")
    all_results["mixed_workload"] = bench_mixed_workload(sample_ids)
    all_results["caveats"].append(
        "Mixed workload writes insert synthetic Person nodes (id >= 10,000,000) and edges; "
        "re-run the loader to reset the dataset to its original 169,924/100,000 state before re-benchmarking."
    )

    print("\n[5/5] Footprint...")
    all_results["footprint"] = bench_footprint(graph)

    with open(RESULTS_PATH, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\nDone. Results written to {RESULTS_PATH}")
    print(json.dumps(all_results, indent=2, default=str))


if __name__ == "__main__":
    main()
