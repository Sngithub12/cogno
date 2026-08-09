
import os
import time
import base64
import json
import pandas as pd

from tqdm import tqdm
from arango import ArangoClient
from dotenv import load_dotenv


# ============================================================
# ArangoDB Cloud Dataset Loader
# ============================================================

print("=" * 60)
print("ArangoDB Cloud Dataset Loader")
print("=" * 60)


# ============================================================
# Configuration
# ============================================================

load_dotenv()

HOST = "https://879864b690d6.arangodb.cloud:18529"
USERNAME = "root"
PASSWORD = os.getenv("ARANGODB_PASSWORD")
ENCODED_CA = os.getenv("encodedCA")

DATABASE = "_system"

NODES_FILE = "data/nodes.csv"
RELATIONSHIPS_FILE = "data/relationships.csv"

BATCH_SIZE = 5000

EXPECTED_NODES = 169924
EXPECTED_RELATIONSHIPS = 100000

NODE_COLLECTION = "Person"
EDGE_COLLECTION = "CONNECTED_TO"

RESULT_FILE = "results/ingest/ingest_arangodb.json"

CERT_FILE = "cert_file.crt"


# ============================================================
# Validation
# ============================================================

if not PASSWORD:
    raise ValueError(
        "ARANGODB_PASSWORD is missing from .env"
    )

if not ENCODED_CA:
    raise ValueError(
        "encodedCA is missing from .env"
    )


# ============================================================
# Overall wall-clock timer
# ============================================================

overall_start = time.perf_counter()


# ============================================================
# Create CA certificate
# ============================================================

print("\nCreating CA certificate...")

try:
    file_content = base64.b64decode(ENCODED_CA)

    with open(CERT_FILE, "wb") as f:
        f.write(file_content)

    print("CA certificate created.")

except Exception as e:
    print("Certificate error:", e)
    raise SystemExit(1)


# ============================================================
# Read dataset
# ============================================================

print("\nReading dataset...")

csv_start = time.perf_counter()

nodes = pd.read_csv(NODES_FILE)
relationships = pd.read_csv(RELATIONSHIPS_FILE)

csv_read_time = time.perf_counter() - csv_start

print(
    f"Nodes CSV          : {len(nodes):,}"
)

print(
    f"Relationships CSV  : {len(relationships):,}"
)

print(
    f"CSV read time      : {csv_read_time:.3f} seconds"
)


# ============================================================
# Validate dataset
# ============================================================

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


required_node_columns = {"id"}

required_relationship_columns = {
    "source",
    "target"
}

if not required_node_columns.issubset(nodes.columns):
    raise ValueError(
        "nodes.csv must contain an 'id' column"
    )

if not required_relationship_columns.issubset(
    relationships.columns
):
    raise ValueError(
        "relationships.csv must contain "
        "'source' and 'target' columns"
    )


# ============================================================
# Connect to ArangoDB Cloud
# ============================================================

print("\nConnecting to ArangoDB Cloud...")

client = ArangoClient(
    hosts=HOST,
    verify_override=CERT_FILE
)

db = client.db(
    DATABASE,
    username=USERNAME,
    password=PASSWORD
)

try:

    print(
        "Connection successful."
    )

    print(
        f"Database: {db.name}"
    )

    print(
        f"Version : {db.version()}"
    )


    # ========================================================
    # Create / verify collections
    # ========================================================

    print("\nChecking Person collection...")

    if not db.has_collection(NODE_COLLECTION):

        db.create_collection(
            NODE_COLLECTION
        )

        print(
            "Person collection created."
        )

    else:

        print(
            "Person collection already exists."
        )


    print(
        "\nChecking CONNECTED_TO edge collection..."
    )

    if not db.has_collection(
        EDGE_COLLECTION
    ):

        db.create_collection(
            EDGE_COLLECTION,
            edge=True
        )

        print(
            "CONNECTED_TO edge collection created."
        )

    else:

        print(
            "CONNECTED_TO edge collection already exists."
        )


    person_collection = db.collection(
        NODE_COLLECTION
    )

    connected_collection = db.collection(
        EDGE_COLLECTION
    )


    # ========================================================
    # Delete existing graph
    # ========================================================

    print("\nDeleting existing graph...")

    delete_start = time.perf_counter()

    person_collection.truncate()
    connected_collection.truncate()

    delete_time = (
        time.perf_counter()
        - delete_start
    )

    print(
        "Existing graph deleted."
    )

    print(
        f"Delete time        : "
        f"{delete_time:.3f} seconds"
    )


    # ========================================================
    # Load nodes
    # ========================================================

    print("\nLoading nodes...")

    node_start = time.perf_counter()

    total_node_batches = (
        len(nodes) + BATCH_SIZE - 1
    ) // BATCH_SIZE

    for start_index in tqdm(
        range(
            0,
            len(nodes),
            BATCH_SIZE
        ),
        total=total_node_batches,
        desc="Nodes"
    ):

        batch = nodes.iloc[
            start_index:
            start_index + BATCH_SIZE
        ]

        documents = [
            {
                "_key": str(int(node_id)),
                "id": int(node_id)
            }
            for node_id in batch["id"]
        ]

        person_collection.import_bulk(
            documents,
            on_duplicate="replace"
        )

    node_time = (
        time.perf_counter()
        - node_start
    )

    node_throughput = (
        EXPECTED_NODES / node_time
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

    node_verify_start = time.perf_counter()

    node_count = person_collection.count()

    node_verify_time = (
        time.perf_counter()
        - node_verify_start
    )

    print(
        f"Nodes currently in ArangoDB: "
        f"{node_count:,}"
    )

    if node_count != EXPECTED_NODES:

        raise RuntimeError(
            f"Node count mismatch. "
            f"Expected {EXPECTED_NODES:,}, "
            f"got {node_count:,}"
        )


    # ========================================================
    # Create Person.id index
    # ========================================================

    print(
        "\nCreating Person.id index..."
    )

    index_start = time.perf_counter()

    index_created = False
    index_verified = False

    try:

        existing_indexes = (
            person_collection.indexes()
        )

        already_exists = False

        for index in existing_indexes:

            fields = index.get(
                "fields",
                []
            )

            if fields == ["id"]:

                already_exists = True

                print(
                    "Person.id index already exists."
                )

                break

        if not already_exists:

            person_collection.add_index(
                {
                    "type": "persistent",
                    "fields": ["id"],
                    "unique": True
                }
            )

            index_created = True

            print(
                "Person.id index created."
            )

    except Exception as e:

        print(
            "Index creation warning:"
        )

        print(e)


    # ========================================================
    # Verify Person.id index
    # ========================================================

    print(
        "\nVerifying Person.id index..."
    )

    try:

        indexes = person_collection.indexes()

        for index in indexes:

            fields = index.get(
                "fields",
                []
            )

            if fields == ["id"]:

                index_verified = True

                print(
                    "Person.id index verified."
                )

                break

        if not index_verified:

            print(
                "WARNING: Person.id index "
                "could not be verified."
            )

    except Exception as e:

        print(
            "Index verification warning:"
        )

        print(e)


    index_time = (
        time.perf_counter()
        - index_start
    )

    print(
        f"Index operation time: "
        f"{index_time:.3f} seconds"
    )


    # ========================================================
    # Load relationships
    # ========================================================

    print(
        "\nLoading relationships..."
    )

    relationship_start = (
        time.perf_counter()
    )

    total_relationship_batches = (
        len(relationships)
        + BATCH_SIZE
        - 1
    ) // BATCH_SIZE

    for start_index in tqdm(
        range(
            0,
            len(relationships),
            BATCH_SIZE
        ),
        total=total_relationship_batches,
        desc="Relationships"
    ):

        batch = relationships.iloc[
            start_index:
            start_index + BATCH_SIZE
        ]

        edges = []

        for row in batch.itertuples(
            index=False
        ):

            source = int(row.source)
            target = int(row.target)

            edges.append(
                {
                    "_from": (
                        f"{NODE_COLLECTION}/{source}"
                    ),
                    "_to": (
                        f"{NODE_COLLECTION}/{target}"
                    )
                }
            )

        connected_collection.import_bulk(
            edges
        )

    relationship_time = (
        time.perf_counter()
        - relationship_start
    )

    relationship_throughput = (
        EXPECTED_RELATIONSHIPS
        / relationship_time
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
        f"{relationship_throughput:,.2f} "
        f"relationships/sec"
    )


    # ========================================================
    # Final verification
    # ========================================================

    print(
        "\nFinal verification..."
    )

    verification_start = (
        time.perf_counter()
    )

    final_node_count = (
        person_collection.count()
    )

    final_relationship_count = (
        connected_collection.count()
    )

    verification_time = (
        time.perf_counter()
        - verification_start
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
        f"Verification   : "
        f"{verification_time:.3f} seconds"
    )


    # ========================================================
    # Final validation
    # ========================================================

    if final_node_count != EXPECTED_NODES:

        raise RuntimeError(
            f"Final node count incorrect. "
            f"Expected {EXPECTED_NODES:,}, "
            f"got {final_node_count:,}"
        )

    if (
        final_relationship_count
        != EXPECTED_RELATIONSHIPS
    ):

        raise RuntimeError(
            f"Final relationship count incorrect. "
            f"Expected {EXPECTED_RELATIONSHIPS:,}, "
            f"got {final_relationship_count:,}"
        )


    # ========================================================
    # Calculate ingestion metrics
    # ========================================================

    data_ingestion_time = (
        node_time
        + relationship_time
    )

    total_records = (
        EXPECTED_NODES
        + EXPECTED_RELATIONSHIPS
    )

    overall_ingest_rate = (
        total_records
        / data_ingestion_time
    )

    overall_wall_clock = (
        time.perf_counter()
        - overall_start
    )


    # ========================================================
    # Print results
    # ========================================================

    print("\n" + "=" * 60)
    print(
        "ArangoDB Ingestion Results"
    )
    print("=" * 60)

    print(
        f"Nodes                  : "
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
        f"{overall_wall_clock:.3f} seconds"
    )

    print(
        f"Index verified         : "
        f"{index_verified}"
    )

    print("=" * 60)


    # ========================================================
    # Save ingestion results
    # ========================================================

    os.makedirs(
        os.path.dirname(RESULT_FILE),
        exist_ok=True
    )

    results = {

        "platform": "ArangoDB Cloud",

        "database": DATABASE,

        "collections": {
            "node_collection": NODE_COLLECTION,
            "edge_collection": EDGE_COLLECTION
        },

        "dataset": {
            "nodes": final_node_count,
            "relationships": final_relationship_count
        },

        "batch_size": BATCH_SIZE,

        "load_method": (
            "Python driver with "
            "ArangoDB import_bulk batching"
        ),

        "csv": {
            "nodes_file": NODES_FILE,
            "relationships_file": RELATIONSHIPS_FILE,
            "read_time_seconds": round(
                csv_read_time,
                3
            )
        },

        "cleanup": {
            "delete_time_seconds": round(
                delete_time,
                3
            )
        },

        "nodes": {
            "load_time_seconds": round(
                node_time,
                3
            ),
            "throughput_nodes_per_sec": round(
                node_throughput,
                2
            )
        },

        "relationships": {
            "load_time_seconds": round(
                relationship_time,
                3
            ),
            "throughput_relationships_per_sec": round(
                relationship_throughput,
                2
            )
        },

        "verification": {
            "node_verification_time_seconds": round(
                node_verify_time,
                3
            ),
            "final_verification_time_seconds": round(
                verification_time,
                3
            )
        },

        "index": {
            "property": "Person.id",
            "type": "persistent",
            "unique": True,
            "created": index_created,
            "verified": index_verified,
            "operation_time_seconds": round(
                index_time,
                3
            )
        },

        "ingestion": {
            "total_records": total_records,
            "data_ingestion_time_seconds": round(
                data_ingestion_time,
                3
            ),
            "overall_ingest_rate_records_per_sec": round(
                overall_ingest_rate,
                2
            ),
            "total_wall_clock_time_seconds": round(
                overall_wall_clock,
                3
            )
        }
    }


    with open(
        RESULT_FILE,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            results,
            f,
            indent=2
        )

    print(
        f"\nResults saved to: "
        f"{RESULT_FILE}"
    )


finally:

    # ========================================================
    # Cleanup
    # ========================================================

    try:
        os.remove(CERT_FILE)
    except OSError:
        pass

    print(
        "\nArangoDB Cloud connection closed."
    )

