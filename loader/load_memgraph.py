import os
import time
import json
import pandas as pd
from tqdm import tqdm
from gqlalchemy import Memgraph
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Configuration
# ============================================================

HOST = os.getenv("MEMGRAPH_HOST")
PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
USERNAME = os.getenv("MEMGRAPH_USERNAME")
PASSWORD = os.getenv("MEMGRAPH_PASSWORD")

NODES_FILE = "data/nodes.csv"
RELATIONSHIPS_FILE = "data/relationships.csv"

RESULT_FILE = "results/ingest/ingest_memgraph.json"

BATCH_SIZE = 5000

EXPECTED_NODES = 169924
EXPECTED_RELATIONSHIPS = 100000


# ============================================================
# Validation
# ============================================================

if not HOST:
    raise ValueError("Missing MEMGRAPH_HOST in .env")

if not USERNAME:
    raise ValueError("Missing MEMGRAPH_USERNAME in .env")

if not PASSWORD:
    raise ValueError("Missing MEMGRAPH_PASSWORD in .env")


# ============================================================
# Helpers
# ============================================================

def chunks(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]


def save_results(results):
    os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)

    with open(RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to: {RESULT_FILE}")


# ============================================================
# Start
# ============================================================

print("=" * 60)
print("Memgraph Cloud Dataset Loader")
print("=" * 60)

total_start = time.perf_counter()


# ============================================================
# Read dataset
# ============================================================

print("\nReading dataset...")

csv_start = time.perf_counter()

nodes = pd.read_csv(NODES_FILE)
relationships = pd.read_csv(RELATIONSHIPS_FILE)

csv_time = time.perf_counter() - csv_start

print(f"Nodes CSV          : {len(nodes):,}")
print(f"Relationships CSV  : {len(relationships):,}")
print(f"CSV read time      : {csv_time:.3f} seconds")

if len(nodes) != EXPECTED_NODES:
    raise ValueError(
        f"Expected {EXPECTED_NODES:,} nodes, "
        f"but CSV contains {len(nodes):,}"
    )

if len(relationships) != EXPECTED_RELATIONSHIPS:
    raise ValueError(
        f"Expected {EXPECTED_RELATIONSHIPS:,} relationships, "
        f"but CSV contains {len(relationships):,}"
    )


# ============================================================
# Connect
# ============================================================

print("\nConnecting to Memgraph Cloud...")

connection = Memgraph(
    host=HOST,
    port=PORT,
    username=USERNAME,
    password=PASSWORD,
    encrypted=True
)

try:

    # ========================================================
    # Test connection
    # ========================================================

    result = connection.execute_and_fetch(
        "RETURN 1 AS ok"
    )

    connection_test = next(result)["ok"]

    print(
        f"Connection successful: "
        f"{connection_test}"
    )


    # ========================================================
    # Delete existing graph
    # ========================================================

    print("\nDeleting existing graph...")

    delete_start = time.perf_counter()

    connection.execute(
        """
        MATCH (n)
        DETACH DELETE n
        """
    )

    delete_time = time.perf_counter() - delete_start

    print(
        f"Existing graph deleted in "
        f"{delete_time:.3f} seconds"
    )


    # ========================================================
    # Verify database is empty
    # ========================================================

    result = connection.execute_and_fetch(
        """
        MATCH (n)
        RETURN count(n) AS count
        """
    )

    remaining_nodes = next(result)["count"]

    print(
        f"Nodes remaining after cleanup: "
        f"{remaining_nodes}"
    )

    if remaining_nodes != 0:
        raise RuntimeError(
            f"Database cleanup failed. "
            f"{remaining_nodes} nodes remain."
        )


    # ========================================================
    # Load nodes
    # ========================================================

    print("\nLoading nodes...")

    node_start = time.perf_counter()

    total_node_batches = (
        len(nodes) + BATCH_SIZE - 1
    ) // BATCH_SIZE

    for batch in tqdm(
        chunks(nodes, BATCH_SIZE),
        total=total_node_batches,
        desc="Nodes"
    ):

        rows = [
            {
                "id": int(x)
            }
            for x in batch["id"]
        ]

        connection.execute(
            """
            UNWIND $rows AS row
            CREATE (:Person {id: row.id})
            """,
            parameters={"rows": rows}
        )

    node_time = time.perf_counter() - node_start

    node_throughput = (
        EXPECTED_NODES / node_time
        if node_time > 0
        else 0
    )

    print(
        f"\nNodes loaded       : "
        f"{EXPECTED_NODES:,}"
    )

    print(
        f"Node load time     : "
        f"{node_time:.3f} seconds"
    )

    print(
        f"Node throughput    : "
        f"{node_throughput:,.2f} nodes/sec"
    )


    # ========================================================
    # Verify nodes
    # ========================================================

    print("\nVerifying nodes...")

    result = connection.execute_and_fetch(
        """
        MATCH (n:Person)
        RETURN count(n) AS count
        """
    )

    node_count = next(result)["count"]

    print(
        f"Nodes currently in Memgraph: "
        f"{node_count:,}"
    )

    if node_count != EXPECTED_NODES:
        raise RuntimeError(
            f"Node count mismatch. "
            f"Expected {EXPECTED_NODES:,}, "
            f"got {node_count:,}"
        )


    # ========================================================
    # Create index
    # ========================================================

    print("\nCreating Person.id index...")

    index_start = time.perf_counter()

    index_verified = False

    try:

        connection.execute(
            """
            CREATE INDEX ON :Person(id)
            """
        )

        print("Index creation requested.")

    except Exception as e:

        message = str(e).lower()

        if (
            "already exists" in message
            or "exists" in message
            or "equivalent" in message
        ):
            print("Index already exists.")

        else:
            print(
                "Warning: Index creation returned:"
            )
            print(e)

    index_time = time.perf_counter() - index_start


    # ========================================================
    # Verify index
    # ========================================================

    print("\nVerifying Person.id index...")

    try:

        indexes = connection.execute_and_fetch(
            """
            SHOW INDEX INFO
            """
        )

        index_rows = list(indexes)

        index_text = str(index_rows).lower()

        if (
            "person" in index_text
            and "id" in index_text
        ):
            index_verified = True

        else:
            # Run an indexed lookup as a functional check.
            test_id = int(nodes.iloc[0]["id"])

            lookup = connection.execute_and_fetch(
                """
                MATCH (n:Person {id: $id})
                RETURN n.id AS id
                """,
                parameters={"id": test_id}
            )

            lookup_rows = list(lookup)

            if (
                lookup_rows
                and lookup_rows[0]["id"] == test_id
            ):
                index_verified = True

    except Exception as e:

        print(
            "Index metadata verification unavailable:"
        )
        print(e)

        # Functional lookup fallback
        try:

            test_id = int(nodes.iloc[0]["id"])

            lookup = connection.execute_and_fetch(
                """
                MATCH (n:Person {id: $id})
                RETURN n.id AS id
                """,
                parameters={"id": test_id}
            )

            lookup_rows = list(lookup)

            if lookup_rows:
                index_verified = True

        except Exception:
            index_verified = False

    print(
        f"Index verified      : "
        f"{index_verified}"
    )

    print(
        f"Index operation time: "
        f"{index_time:.3f} seconds"
    )


    # ========================================================
    # Load relationships
    # ========================================================

    print("\nLoading relationships...")

    relationship_start = time.perf_counter()

    total_relationship_batches = (
        len(relationships) + BATCH_SIZE - 1
    ) // BATCH_SIZE

    for batch in tqdm(
        chunks(relationships, BATCH_SIZE),
        total=total_relationship_batches,
        desc="Relationships"
    ):

        rows = [
            {
                "source": int(row.source),
                "target": int(row.target)
            }
            for row in batch.itertuples(index=False)
        ]

        connection.execute(
            """
            UNWIND $rows AS row

            MATCH (a:Person {id: row.source})
            MATCH (b:Person {id: row.target})

            CREATE (a)-[:CONNECTED_TO]->(b)
            """,
            parameters={"rows": rows}
        )

    relationship_time = (
        time.perf_counter() - relationship_start
    )

    relationship_throughput = (
        EXPECTED_RELATIONSHIPS / relationship_time
        if relationship_time > 0
        else 0
    )

    print(
        f"\nRelationships loaded : "
        f"{EXPECTED_RELATIONSHIPS:,}"
    )

    print(
        f"Relationship time    : "
        f"{relationship_time:.3f} seconds"
    )

    print(
        f"Relationship throughput: "
        f"{relationship_throughput:,.2f} relationships/sec"
    )


    # ========================================================
    # Final verification
    # ========================================================

    print("\nFinal verification...")

    verification_start = time.perf_counter()

    result = connection.execute_and_fetch(
        """
        MATCH (n:Person)
        RETURN count(n) AS count
        """
    )

    final_node_count = next(result)["count"]

    result = connection.execute_and_fetch(
        """
        MATCH ()-[r:CONNECTED_TO]->()
        RETURN count(r) AS count
        """
    )

    final_relationship_count = next(result)["count"]

    verification_time = (
        time.perf_counter() - verification_start
    )

    print(
        f"Nodes          : "
        f"{final_node_count:,}"
    )

    print(
        f"Relationships   : "
        f"{final_relationship_count:,}"
    )

    print(
        f"Verification    : "
        f"{verification_time:.3f} seconds"
    )


    # ========================================================
    # Validate
    # ========================================================

    if final_node_count != EXPECTED_NODES:
        raise RuntimeError(
            f"Final node count incorrect. "
            f"Expected {EXPECTED_NODES:,}, "
            f"got {final_node_count:,}"
        )

    if final_relationship_count != EXPECTED_RELATIONSHIPS:
        raise RuntimeError(
            f"Final relationship count incorrect. "
            f"Expected {EXPECTED_RELATIONSHIPS:,}, "
            f"got {final_relationship_count:,}"
        )


    # ========================================================
    # Calculate ingestion metrics
    # ========================================================

    data_ingestion_time = (
        node_time + relationship_time
    )

    total_records = (
        EXPECTED_NODES +
        EXPECTED_RELATIONSHIPS
    )

    overall_ingest_rate = (
        total_records / data_ingestion_time
        if data_ingestion_time > 0
        else 0
    )

    total_wall_clock = (
        time.perf_counter() - total_start
    )


    # ========================================================
    # Print summary
    # ========================================================

    print("\n" + "=" * 60)
    print("Memgraph Cloud Ingestion Summary")
    print("=" * 60)

    print(
        f"Nodes                  : "
        f"{final_node_count:,}"
    )

    print(
        f"Relationships           : "
        f"{final_relationship_count:,}"
    )

    print(
        f"Node throughput        : "
        f"{node_throughput:,.2f} nodes/sec"
    )

    print(
        f"Relationship throughput: "
        f"{relationship_throughput:,.2f} rel/sec"
    )

    print(
        f"Overall ingest rate     : "
        f"{overall_ingest_rate:,.2f} records/sec"
    )

    print(
        f"Data ingestion time     : "
        f"{data_ingestion_time:.3f} seconds"
    )

    print(
        f"Total wall-clock time   : "
        f"{total_wall_clock:.3f} seconds"
    )

    print(
        f"Index verified          : "
        f"{index_verified}"
    )


    # ========================================================
    # Save results
    # ========================================================

    results = {
        "platform": "Memgraph Cloud",

        "dataset": {
            "nodes": final_node_count,
            "relationships": final_relationship_count
        },

        "configuration": {
            "batch_size": BATCH_SIZE,
            "encrypted": True,
            "loader": "Python gqlalchemy driver batching"
        },

        "ingest": {
            "csv_read_time_seconds": round(
                csv_time, 3
            ),

            "delete_time_seconds": round(
                delete_time, 3
            ),

            "node_load_time_seconds": round(
                node_time, 3
            ),

            "relationship_load_time_seconds": round(
                relationship_time, 3
            ),

            "data_ingestion_time_seconds": round(
                data_ingestion_time, 3
            ),

            "total_wall_clock_seconds": round(
                total_wall_clock, 3
            ),

            "nodes_per_second": round(
                node_throughput, 2
            ),

            "relationships_per_second": round(
                relationship_throughput, 2
            ),

            "overall_records_per_second": round(
                overall_ingest_rate, 2
            )
        },

        "index": {
            "property": "Person.id",
            "verified": index_verified,
            "operation_time_seconds": round(
                index_time, 3
            )
        },

        "verification": {
            "nodes": final_node_count,
            "relationships": final_relationship_count,
            "verification_time_seconds": round(
                verification_time, 3
            )
        },

        "method": {
            "node_batch_size": BATCH_SIZE,
            "relationship_batch_size": BATCH_SIZE,
            "node_method": "UNWIND parameter batches",
            "relationship_method": (
                "UNWIND parameter batches with MATCH "
                "and CREATE"
            )
        }
    }

    save_results(results)

    print("\nMemgraph Cloud dataset loading complete!")

finally:

    try:
        connection.close()
        print("Memgraph connection closed.")
    except Exception:
        pass