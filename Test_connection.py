from neo4j import GraphDatabase

from dotenv import load_dotenv
import os

load_dotenv()

URI = os.getenv("COGNODB_URI")
USERNAME = os.getenv("COGNODB_USERNAME")
PASSWORD = os.getenv("COGNODB_PASSWORD")
driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)

def test_query(tx):
    result = tx.run("RETURN 'Hello CognoDB!' AS message")
    return result.single()["message"]

with driver.session() as session:
    message = session.execute_read(test_query)
    print(message)

driver.close()

