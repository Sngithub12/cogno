import os
import time
import json
import pandas as pd
from tqdm import tqdm
from neo4j import GraphDatabase
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

load_dotenv()

URI = os.getenv("COGNODB_URI")
USERNAME = os.getenv("COGNODB_USERNAME")
PASSWORD = os.getenv("COGNODB_PASSWORD")

NODES_FILE = "data/nodes.csv"
RELATIONSHIPS_FILE = "data/relationships.csv"

RESULTS_FILE = "results/ingest/ingest_cognodb.json"

BATCH_SIZE = 5000

EXPECTED_NODES = 169924
EXPECTED_RELATIONSHIPS = 100000

INDEX_NAME = "person_id_index"


# ============================================================
# Validation
# ============================================================

if not URI:
    raise ValueError("Missing COGNODB_URI in .env")

if not USERNAME:
    raise ValueError("Missing COGNODB_USERNAME in .env")

if not PASSWORD:
    raise ValueError("Missing COGNODB_PASSWORD in .env")


# ============================================================
# Helpers
# ============================================================

def chunks(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]


def save_results(results):
    os.makedirs(
        os.path.dirname(RESULTS_FILE),
        exist_ok=True
    )

    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(
            results,
            f,
            indent=2
        )


# ============================================================
# Main
# ============================================================

print("=" * 60)
print("CognoDB Dataset Loader")
print("=" * 60)


# ============================================================
# Read dataset
# ============================================================

print("\nReading dataset...")

csv_start = time.perf_counter()

nodes = pd.read_csv(NODES_FILE)
relationships = pd.read_csv(RELATIONSHIPS_FILE)

csv_time = time.perf_counter() - csv_start

print(
    f"Nodes CSV          : {len(nodes):,}"
)

print(
    f"Relationships CSV  : {len(relationships):,}"
)

print(
    f"CSV read time      : {csv_time:.3f} seconds"
)


# ============================================================
# Validate CSV
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
        f"nodes.csv must contain columns: "
        f"{required_node_columns}"
    )

if not required_relationship_columns.issubset(
    relationships.columns
):
    raise ValueError(
        "relationships.csv must contain columns: "
        f"{required_relationship_columns}"
    )


# ============================================================
# Connect
# ============================================================

print("\nConnecting to CognoDB...")

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)


results = {
    "platform": "CognoDB",
    "dataset": {
        "nodes": EXPECTED_NODES,
        "relationships": EXPECTED_RELATIONSHIPS
    },
    "loader": {
        "method": "Python Neo4j-compatible driver with UNWIND batching",
        "batch_size": BATCH_SIZE,
        "nodes_file": NODES_FILE,
        "relationships_file": RELATIONSHIPS_FILE
    },
    "timing": {},
    "throughput": {},
    "verification": {},
    "index": {}
}


try:

    with driver.session() as session:

        # ====================================================
        # Connection test
        # ====================================================

        result = session.run(
            "RETURN 1 AS value"
        ).single()

        print(
            f"Connection successful: "
            f"{result['value']}"
        )


        # ====================================================
        # Total wall-clock timer
        # ====================================================

        total_start = time.perf_counter()


        # ====================================================
        # Delete existing graph
        # ====================================================

        print("\nDeleting existing graph...")

        delete_start = time.perf_counter()

        session.run(
            """
            MATCH (n)
            DETACH DELETE n
            """
        ).consume()

        delete_time = (
            time.perf_counter() - delete_start
        )

        print(
            f"Existing graph deleted in "
            f"{delete_time:.3f} seconds"
        )

        results["timing"]["cleanup_seconds"] = round(
            delete_time,
            3
        )


        # ====================================================
        # Verify empty database
        # ====================================================

        result = session.run(
            """
            MATCH (n)
            RETURN count(n) AS count
            """
        ).single()

        remaining_nodes = result["count"]

        print(
            f"Nodes remaining after cleanup: "
            f"{remaining_nodes:,}"
        )

        if remaining_nodes != 0:
            raise RuntimeError(
                "Database cleanup failed. "
                f"{remaining_nodes:,} nodes still exist."
            )


        # ====================================================
        # Load nodes
        # ====================================================

        print("\nLoading nodes...")

        node_start = time.perf_counter()

        total_batches = (
            len(nodes) + BATCH_SIZE - 1
        ) // BATCH_SIZE

        for batch in tqdm(
            chunks(nodes, BATCH_SIZE),
            total=total_batches,
            desc="Nodes"
        ):

            rows = [
                {
                    "id": int(row_id)
                }
                for row_id in batch["id"]
            ]

            session.run(
                """
                UNWIND $rows AS row
                CREATE (:Person {id: row.id})
                """,
                rows=rows
            ).consume()

        node_time = (
            time.perf_counter() - node_start
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

        results["timing"]["node_load_seconds"] = round(
            node_time,
            3
        )

        results["throughput"]["nodes_per_second"] = round(
            node_throughput,
            2
        )


        # ====================================================
        # Verify nodes
        # ====================================================

        print("\nVerifying nodes...")

        verify_nodes_start = time.perf_counter()

        node_count = session.run(
            """
            MATCH (n:Person)
            RETURN count(n) AS count
            """
        ).single()["count"]

        verify_nodes_time = (
            time.perf_counter()
            - verify_nodes_start
        )

        print(
            f"Nodes currently in CognoDB: "
            f"{node_count:,}"
        )

        if node_count != EXPECTED_NODES:
            raise RuntimeError(
                f"Node count mismatch. "
                f"Expected {EXPECTED_NODES:,}, "
                f"got {node_count:,}"
            )


        # ====================================================
        # Create Person.id index
        # ====================================================

        print("\nCreating Person.id index...")

        index_start = time.perf_counter()

        index_created = False

        try:

            # Modern Neo4j-compatible syntax
            session.run(
                f"""
                CREATE INDEX {INDEX_NAME}
                FOR (n:Person)
                ON (n.id)
                """
            ).consume()

            index_created = True

            print(
                "Index created successfully."
            )

        except Exception as e:

            message = str(e).lower()

            if (
                "already exists" in message
                or "equivalent" in message
                or "already" in message
            ):

                print(
                    "Index already exists."
                )

            else:

                print(
                    "Warning: Could not create index:"
                )

                print(e)


        index_time = (
            time.perf_counter() - index_start
        )

        print(
            f"Index operation time: "
            f"{index_time:.3f} seconds"
        )


        # ====================================================
        # Verify index
        # ====================================================

        print("\nVerifying Person.id index...")

        index_verified = False

        try:

            index_result = session.run(
                """
                SHOW INDEXES
                """
            )

            indexes = list(index_result)

            for index in indexes:

                index_text = str(index).lower()

                if (
                    "person" in index_text
                    and "id" in index_text
                ):
                    index_verified = True
                    break

        except Exception as e:

            print(
                "Could not automatically verify index:"
            )

            print(e)


        if index_verified:
            print(
                "Person.id index verified."
            )
        else:
            print(
                "WARNING: Person.id index "
                "could not be verified."
            )


        results["index"] = {
            "name": INDEX_NAME,
            "property": "Person.id",
            "created_during_run": index_created,
            "verified": index_verified,
            "operation_time_seconds": round(
                index_time,
                3
            )
        }


        # ====================================================
        # Load relationships
        # ====================================================

        print("\nLoading relationships...")

        relationship_start = time.perf_counter()

        total_batches = (
            len(relationships)
            + BATCH_SIZE
            - 1
        ) // BATCH_SIZE

        for batch in tqdm(
            chunks(
                relationships,
                BATCH_SIZE
            ),
            total=total_batches,
            desc="Relationships"
        ):

            rows = [
                {
                    "source": int(row.source),
                    "target": int(row.target)
                }
                for row in batch.itertuples(
                    index=False
                )
            ]

            session.run(
                """
                UNWIND $rows AS row

                MATCH (a:Person {id: row.source})
                MATCH (b:Person {id: row.target})

                CREATE (a)-[:CONNECTED_TO]->(b)
                """,
                rows=rows
            ).consume()

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

        results["timing"][
            "relationship_load_seconds"
        ] = round(
            relationship_time,
            3
        )

        results["throughput"][
            "relationships_per_second"
        ] = round(
            relationship_throughput,
            2
        )


        # ====================================================
        # Final verification
        # ====================================================

        print("\nFinal verification...")

        verification_start = time.perf_counter()

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

        verification_time = (
            time.perf_counter()
            - verification_start
        )

        print(
            f"Nodes          : "
            f"{node_count:,}"
        )

        print(
            f"Relationships   : "
            f"{relationship_count:,}"
        )

        print(
            f"Verification    : "
            f"{verification_time:.3f} seconds"
        )


        # ====================================================
        # Validate final dataset
        # ====================================================

        if node_count != EXPECTED_NODES:

            raise RuntimeError(
                f"Final node count incorrect: "
                f"{node_count:,}"
            )

        if relationship_count != EXPECTED_RELATIONSHIPS:

            raise RuntimeError(
                f"Final relationship count incorrect: "
                f"{relationship_count:,}"
            )


        # ====================================================
        # Calculate ingestion metrics
        # ====================================================

        data_ingestion_time = (
            node_time
            + relationship_time
        )

        total_wall_clock_time = (
            time.perf_counter()
            - total_start
        )

        total_records = (
            EXPECTED_NODES
            + EXPECTED_RELATIONSHIPS
        )

        overall_ingest_rate = (
            total_records
            / data_ingestion_time
        )

        results["timing"][
            "verification_seconds"
        ] = round(
            verification_time,
            3
        )

        results["timing"][
            "data_ingestion_seconds"
        ] = round(
            data_ingestion_time,
            3
        )

        results["timing"][
            "total_wall_clock_seconds"
        ] = round(
            total_wall_clock_time,
            3
        )

        results["throughput"][
            "overall_records_per_second"
        ] = round(
            overall_ingest_rate,
            2
        )

        results["verification"] = {
            "nodes": node_count,
            "relationships": relationship_count,
            "expected_nodes": EXPECTED_NODES,
            "expected_relationships": EXPECTED_RELATIONSHIPS,
            "passed": (
                node_count == EXPECTED_NODES
                and
                relationship_count
                == EXPECTED_RELATIONSHIPS
            )
        }


        # ====================================================
        # Final output
        # ====================================================

        print("\n" + "=" * 60)
        print(
            "CognoDB dataset loading complete!"
        )
        print("=" * 60)

        print(
            f"Nodes                  : "
            f"{node_count:,}"
        )

        print(
            f"Relationships           : "
            f"{relationship_count:,}"
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
            f"Index verified        : "
            f"{index_verified}"
        )


        # ====================================================
        # Save result
        # ====================================================

        save_results(results)

        print(
            f"\nResults saved to: "
            f"{RESULTS_FILE}"
        )


finally:

    driver.close()

    print("\nCognoDB connection closed.")