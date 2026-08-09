import os
import time
import json
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from falkordb import FalkorDB

# ============================================================
# Configuration
# ============================================================

load_dotenv()

HOST = os.getenv("FALKORDB_HOST")
PORT = int(os.getenv("FALKORDB_PORT", "57891"))
USERNAME = os.getenv("FALKORDB_USERNAME", "falkordb")
PASSWORD = os.getenv("FALKORDB_PASSWORD")

GRAPH_NAME = "cogno_benchmark"

NODES_FILE = "data/nodes.csv"
RELATIONSHIPS_FILE = "data/relationships.csv"

BATCH_SIZE = 500

EXPECTED_NODES = 169924
EXPECTED_RELATIONSHIPS = 100000

RESULTS_FILE = "results/ingest/ingest_falkordb.json"


# ============================================================
# Validation
# ============================================================

if not HOST:
    raise ValueError("Missing FALKORDB_HOST in .env")

if not PASSWORD:
    raise ValueError("Missing FALKORDB_PASSWORD in .env")


# ============================================================
# Start timer
# ============================================================

wall_start = time.perf_counter()

print("=" * 60)
print("FalkorDB Dataset Loader")
print("=" * 60)


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
        f"Expected {EXPECTED_NODES} nodes, "
        f"but found {len(nodes)}"
    )

if len(relationships) != EXPECTED_RELATIONSHIPS:
    raise ValueError(
        f"Expected {EXPECTED_RELATIONSHIPS} relationships, "
        f"but found {len(relationships)}"
    )


# ============================================================
# Connect
# ============================================================

print("\nConnecting to FalkorDB...")

db = FalkorDB(
    host=HOST,
    port=PORT,
    username=USERNAME,
    password=PASSWORD,
)

graph = db.select_graph(GRAPH_NAME)

try:
    ping_result = graph.query("RETURN 1")
    print("Connection successful:", ping_result.result_set)

except Exception as e:
    print("Connection failed:", e)
    raise


# ============================================================
# Clear existing graph
# ============================================================

print("\nDeleting existing graph...")

delete_start = time.perf_counter()

try:
    graph.delete()
    print("Existing graph deleted.")

except Exception as e:
    print("Graph did not exist or delete returned:")
    print(e)

delete_time = time.perf_counter() - delete_start

print(f"Delete time        : {delete_time:.3f} seconds")


# Re-select graph after deletion
graph = db.select_graph(GRAPH_NAME)


# ============================================================
# Load nodes
# ============================================================

print("\nLoading nodes...")

node_start = time.perf_counter()

total_node_batches = (
    len(nodes) + BATCH_SIZE - 1
) // BATCH_SIZE


for batch_start in tqdm(
    range(0, len(nodes), BATCH_SIZE),
    total=total_node_batches,
    desc="Nodes"
):

    batch_df = nodes.iloc[
        batch_start:batch_start + BATCH_SIZE
    ]

    rows = [
        {
            "id": int(x)
        }
        for x in batch_df["id"]
    ]

    graph.query(
        """
        UNWIND $rows AS row
        CREATE (:Person {id: row.id})
        RETURN count(*)
        """,
        {"rows": rows}
    )


node_time = time.perf_counter() - node_start

node_throughput = (
    EXPECTED_NODES / node_time
    if node_time > 0
    else 0
)

print(f"\nNodes loaded       : {EXPECTED_NODES:,}")
print(f"Node load time     : {node_time:.3f} seconds")
print(f"Node throughput    : {node_throughput:,.2f} nodes/sec")


# ============================================================
# Verify nodes
# ============================================================

print("\nVerifying nodes...")

verify_node_start = time.perf_counter()

result = graph.query(
    """
    MATCH (n:Person)
    RETURN count(n)
    """
)

node_count = result.result_set[0][0]

verify_node_time = (
    time.perf_counter() - verify_node_start
)

print(
    f"Nodes currently in FalkorDB: "
    f"{node_count:,}"
)

if node_count != EXPECTED_NODES:
    raise RuntimeError(
        f"Node count mismatch. "
        f"Expected {EXPECTED_NODES}, "
        f"got {node_count}"
    )


# ============================================================
# Create index
# ============================================================

print("\nCreating Person.id index...")

index_start = time.perf_counter()

index_verified = False

try:

    graph.query(
        """
        CREATE INDEX FOR (n:Person) ON (n.id)
        """
    )

    print("Index creation requested.")

except Exception as e:

    message = str(e).lower()

    if (
        "already exists" in message
        or "already indexed" in message
        or "exists" in message
    ):
        print("Index already exists.")

    else:
        print("Index creation returned:")
        print(e)


index_time = time.perf_counter() - index_start


# ============================================================
# Verify index
# ============================================================

print("\nVerifying Person.id index...")

try:

    index_result = graph.query(
        """
        CALL db.indexes()
        """
    )

    index_text = str(index_result.result_set)

    if (
        "Person" in index_text
        and "id" in index_text
    ):
        index_verified = True
        print("Person.id index verified.")

    else:
        print(
            "Warning: Person.id index could not "
            "be confirmed from db.indexes()."
        )

except Exception as e:

    print(
        "Warning: Could not inspect indexes:"
    )
    print(e)


print(
    f"Index operation time: "
    f"{index_time:.3f} seconds"
)


# ============================================================
# Load relationships
# ============================================================

print("\nLoading relationships...")

relationship_start = time.perf_counter()

total_relationship_batches = (
    len(relationships) + BATCH_SIZE - 1
) // BATCH_SIZE


for batch_start in tqdm(
    range(0, len(relationships), BATCH_SIZE),
    total=total_relationship_batches,
    desc="Relationships"
):

    batch_df = relationships.iloc[
        batch_start:batch_start + BATCH_SIZE
    ]

    rows = [
        {
            "source": int(row.source),
            "target": int(row.target)
        }
        for row in batch_df.itertuples(
            index=False
        )
    ]

    graph.query(
        """
        UNWIND $rows AS row

        MATCH (a:Person {id: row.source})
        MATCH (b:Person {id: row.target})

        CREATE (a)-[:CONNECTED_TO]->(b)

        RETURN count(*)
        """,
        {"rows": rows}
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


# ============================================================
# Final verification
# ============================================================

print("\nFinal verification...")

verification_start = time.perf_counter()

node_result = graph.query(
    """
    MATCH (n:Person)
    RETURN count(n)
    """
)

relationship_result = graph.query(
    """
    MATCH ()-[r:CONNECTED_TO]->()
    RETURN count(r)
    """
)

final_node_count = node_result.result_set[0][0]

final_relationship_count = (
    relationship_result.result_set[0][0]
)

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


# ============================================================
# Validate
# ============================================================

if final_node_count != EXPECTED_NODES:
    raise RuntimeError(
        f"Final node count incorrect: "
        f"{final_node_count}"
    )

if final_relationship_count != EXPECTED_RELATIONSHIPS:
    raise RuntimeError(
        f"Final relationship count incorrect: "
        f"{final_relationship_count}"
    )


# ============================================================
# Ingest metrics
# ============================================================

data_ingestion_time = (
    node_time +
    relationship_time
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

total_wall_clock_time = (
    time.perf_counter() - wall_start
)


# ============================================================
# Results
# ============================================================

results = {

    "platform": "FalkorDB Cloud",

    "graph_name": GRAPH_NAME,

    "dataset": {
        "nodes": final_node_count,
        "relationships": final_relationship_count
    },

    "ingest": {

        "batch_size": BATCH_SIZE,

        "csv_read_time_seconds": round(
            csv_time,
            3
        ),

        "delete_time_seconds": round(
            delete_time,
            3
        ),

        "nodes": {
            "count": EXPECTED_NODES,
            "time_seconds": round(
                node_time,
                3
            ),
            "nodes_per_second": round(
                node_throughput,
                2
            )
        },

        "relationships": {
            "count": EXPECTED_RELATIONSHIPS,
            "time_seconds": round(
                relationship_time,
                3
            ),
            "relationships_per_second": round(
                relationship_throughput,
                2
            )
        },

        "total_records": total_records,

        "data_ingestion_time_seconds": round(
            data_ingestion_time,
            3
        ),

        "overall_ingest_records_per_second": round(
            overall_ingest_rate,
            2
        ),

        "total_wall_clock_time_seconds": round(
            total_wall_clock_time,
            3
        )
    },

    "index": {
        "property": "Person.id",
        "verified": index_verified,
        "operation_time_seconds": round(
            index_time,
            3
        )
    },

    "verification": {
        "nodes": final_node_count,
        "relationships": final_relationship_count,
        "verification_time_seconds": round(
            verification_time,
            3
        )
    },

    "method": {
        "node_load_method": (
            "Python FalkorDB driver with UNWIND "
            "batched CREATE"
        ),
        "relationship_load_method": (
            "Python FalkorDB driver with UNWIND "
            "batched MATCH + CREATE"
        ),
        "batch_size": BATCH_SIZE
    }
}


# ============================================================
# Save results
# ============================================================

os.makedirs(
    os.path.dirname(RESULTS_FILE),
    exist_ok=True
)

with open(
    RESULTS_FILE,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        results,
        f,
        indent=2
    )


# ============================================================
# Final output
# ============================================================

print("\n" + "=" * 60)
print("FalkorDB dataset loading complete!")
print("=" * 60)

print(
    f"\nNodes                  : "
    f"{final_node_count:,}"
)

print(
    f"Relationships          : "
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
    f"Overall ingest rate    : "
    f"{overall_ingest_rate:,.2f} records/sec"
)

print(
    f"Data ingestion time    : "
    f"{data_ingestion_time:.3f} seconds"
)

print(
    f"Total wall-clock time  : "
    f"{total_wall_clock_time:.3f} seconds"
)

print(
    f"Index verified         : "
    f"{index_verified}"
)

print(
    f"\nResults saved to: "
    f"{RESULTS_FILE}"
)

print("\nFalkorDB connection complete.")