
import os
import time
import json
import pandas as pd
from tqdm import tqdm
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Neo4j Aura Dataset Loader
# ============================================================

print("=" * 60)
print("Neo4j Aura Dataset Loader")
print("=" * 60)

# ============================================================
# Configuration
# ============================================================

URI = os.getenv("NEO4J_AURA_URI")
USERNAME = os.getenv("NEO4J_AURA_USERNAME")
PASSWORD = os.getenv("NEO4J_AURA_PASSWORD")

NODES_FILE = "data/nodes.csv"
RELATIONSHIPS_FILE = "data/relationships.csv"

RESULTS_DIR = "results/ingest"
RESULTS_FILE = os.path.join(
    RESULTS_DIR,
    "ingest_neo4jaura.json"
)

BATCH_SIZE = 5000

EXPECTED_NODES = 169924
EXPECTED_RELATIONSHIPS = 100000

# ============================================================
# Validation
# ============================================================

if not URI:
    raise ValueError("Missing NEO4J_AURA_URI in .env")

if not USERNAME:
    raise ValueError("Missing NEO4J_AURA_USERNAME in .env")

if not PASSWORD:
    raise ValueError("Missing NEO4J_AURA_PASSWORD in .env")


# ============================================================
# Helpers
# ============================================================

def chunks(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]


# ============================================================
# Overall timer
# ============================================================

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


# ============================================================
# Connect
# ============================================================

print("\nConnecting to Neo4j Aura...")

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)

delete_time = 0.0
index_time = 0.0
node_time = 0.0
relationship_time = 0.0
verification_time = 0.0
index_verified = False

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
            time.perf_counter()
            - delete_start
        )

        print(
            f"Existing graph deleted in "
            f"{delete_time:.3f} seconds"
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
            f"{remaining_nodes}"
        )

        if remaining_nodes != 0:
            raise RuntimeError(
                "Database cleanup failed. "
                f"{remaining_nodes} nodes remain."
            )


        # ====================================================
        # Create Person.id constraint
        # ====================================================

        print(
            "\nCreating Person.id constraint..."
        )

        index_start = time.perf_counter()

        try:

            session.run(
                """
                CREATE CONSTRAINT person_id_unique
                IF NOT EXISTS
                FOR (n:Person)
                REQUIRE n.id IS UNIQUE
                """
            ).consume()

            print(
                "Person.id constraint created/verified."
            )

        except Exception as e:

            print(
                "Warning while creating constraint:"
            )
            print(e)


        # ====================================================
        # Verify Person.id constraint
        # ====================================================

        try:

            constraints = list(
                session.run(
                    """
                    SHOW CONSTRAINTS
                    """
                )
            )

            for record in constraints:

                data = record.data()
                text = str(data).lower()

                if (
                    "person_id_unique" in text
                    or (
                        "person" in text
                        and "id" in text
                    )
                ):
                    index_verified = True
                    break

        except Exception as e:

            print(
                "Warning: Could not verify "
                "Person.id constraint:"
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

        print(
            f"Index verified      : "
            f"{index_verified}"
        )


        # ====================================================
        # Load nodes
        # ====================================================

        print("\nLoading nodes...")

        node_start = time.perf_counter()

        total_node_batches = (
            len(nodes)
            + BATCH_SIZE
            - 1
        ) // BATCH_SIZE

        for batch in tqdm(
            chunks(nodes, BATCH_SIZE),
            total=total_node_batches,
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

                CREATE (:Person {
                    id: row.id
                })
                """,
                rows=rows
            ).consume()

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


        # ====================================================
        # Verify nodes
        # ====================================================

        print("\nVerifying nodes...")

        node_count = session.run(
            """
            MATCH (n:Person)
            RETURN count(n) AS count
            """
        ).single()["count"]

        print(
            f"Nodes currently in Neo4j Aura: "
            f"{node_count:,}"
        )

        if node_count != EXPECTED_NODES:
            raise RuntimeError(
                "Node count mismatch. "
                f"Expected {EXPECTED_NODES:,}, "
                f"got {node_count:,}"
            )


        # ====================================================
        # Load relationships
        # ====================================================

        print("\nLoading relationships...")

        relationship_start = time.perf_counter()

        total_relationship_batches = (
            len(relationships)
            + BATCH_SIZE
            - 1
        ) // BATCH_SIZE

        for batch in tqdm(
            chunks(
                relationships,
                BATCH_SIZE
            ),
            total=total_relationship_batches,
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

                MATCH (
                    a:Person {
                        id: row.source
                    }
                )

                MATCH (
                    b:Person {
                        id: row.target
                    }
                )

                CREATE (
                    a
                )-[:CONNECTED_TO]->(
                    b
                )
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
            f"{relationship_throughput:,.2f} relationships/sec"
        )


        # ====================================================
        # Final verification
        # ====================================================

        print("\nFinal verification...")

        verification_start = time.perf_counter()

        final_node_count = session.run(
            """
            MATCH (n:Person)
            RETURN count(n) AS count
            """
        ).single()["count"]

        final_relationship_count = session.run(
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


        # ====================================================
        # Validate final dataset
        # ====================================================

        if final_node_count != EXPECTED_NODES:
            raise RuntimeError(
                "Final node count incorrect: "
                f"{final_node_count:,}"
            )

        if final_relationship_count != EXPECTED_RELATIONSHIPS:
            raise RuntimeError(
                "Final relationship count incorrect: "
                f"{final_relationship_count:,}"
            )


        # ====================================================
        # Calculate ingestion metrics
        # ====================================================

        total_records = (
            EXPECTED_NODES
            + EXPECTED_RELATIONSHIPS
        )

        data_ingestion_time = (
            node_time
            + relationship_time
        )

        overall_ingest_rate = (
            total_records
            / data_ingestion_time
        )

        total_wall_clock_time = (
            time.perf_counter()
            - total_start
        )


        # ====================================================
        # Print summary
        # ====================================================

        print("\n" + "=" * 60)
        print("Neo4j Aura Ingestion Summary")
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

        print("=" * 60)


        # ====================================================
        # Save ingest results
        # ====================================================

        os.makedirs(
            RESULTS_DIR,
            exist_ok=True
        )

        results = {

            "platform": "Neo4j AuraDB",

            "dataset": {
                "nodes": final_node_count,
                "relationships": final_relationship_count
            },

            "configuration": {
                "batch_size": BATCH_SIZE,
                "load_method": (
                    "Python Neo4j driver "
                    "with UNWIND batching"
                ),
                "node_batches": total_node_batches,
                "relationship_batches": (
                    total_relationship_batches
                )
            },

            "timing": {

                "csv_read_seconds": round(
                    csv_time,
                    3
                ),

                "delete_seconds": round(
                    delete_time,
                    3
                ),

                "index_operation_seconds": round(
                    index_time,
                    3
                ),

                "node_load_seconds": round(
                    node_time,
                    3
                ),

                "relationship_load_seconds": round(
                    relationship_time,
                    3
                ),

                "verification_seconds": round(
                    verification_time,
                    3
                ),

                "data_ingestion_seconds": round(
                    data_ingestion_time,
                    3
                ),

                "total_wall_clock_seconds": round(
                    total_wall_clock_time,
                    3
                )
            },

            "throughput": {

                "nodes_per_second": round(
                    node_throughput,
                    2
                ),

                "relationships_per_second": round(
                    relationship_throughput,
                    2
                ),

                "overall_records_per_second": round(
                    overall_ingest_rate,
                    2
                )
            },

            "index": {
                "property": "Person.id",
                "type": "UNIQUE constraint",
                "verified": index_verified
            },

            "verification": {

                "nodes": final_node_count,

                "relationships":
                    final_relationship_count,

                "valid": (
                    final_node_count
                    == EXPECTED_NODES
                    and
                    final_relationship_count
                    == EXPECTED_RELATIONSHIPS
                )
            }
        }

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

        print(
            f"\nResults saved to: "
            f"{RESULTS_FILE}"
        )

finally:

    driver.close()

    print(
        "\nNeo4j Aura connection closed."
    )

