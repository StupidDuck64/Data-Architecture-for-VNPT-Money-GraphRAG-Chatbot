import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")


class Neo4jConnection:
    """Singleton quản lý kết nối Neo4j."""
    
    _instance = None
    _driver = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                max_connection_lifetime=3600,
                max_connection_pool_size=50,
                connection_acquisition_timeout=60
            )
    
    @property
    def driver(self):
        return self._driver
    
    def close(self):
        if self._driver:
            self._driver.close()
            self._driver = None
    
    def verify_connectivity(self) -> bool:
        try:
            self._driver.verify_connectivity()
            return True
        except Exception as e:
            print(f"Kết nối Neo4j thất bại: {e}")
            return False
    
    def get_session(self, database: str = None):
        return self._driver.session(database=database or NEO4J_DATABASE)
    
    def execute_query(self, query: str, parameters: dict = None, database: str = None):
        with self.get_session(database) as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def execute_write(self, query: str, parameters: dict = None, database: str = None):
        with self.get_session(database) as session:
            result = session.execute_write(
                lambda tx: tx.run(query, parameters or {}).consume()
            )
            return result


_connection = None


def get_neo4j_connection() -> Neo4jConnection:
    global _connection
    if _connection is None:
        _connection = Neo4jConnection()
    return _connection


def get_neo4j_driver():
    return get_neo4j_connection().driver


def close_neo4j_connection():
    global _connection
    if _connection:
        _connection.close()
        _connection = None


def get_node_count(label: str) -> int:
    conn = get_neo4j_connection()
    result = conn.execute_query(f"MATCH (n:{label}) RETURN count(n) AS count")
    return result[0]["count"] if result else 0


def get_relationship_count(rel_type: str) -> int:
    conn = get_neo4j_connection()
    result = conn.execute_query(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count")
    return result[0]["count"] if result else 0


def get_graph_stats() -> dict:
    conn = get_neo4j_connection()
    
    node_query = """
    MATCH (n)
    RETURN labels(n)[0] AS label, count(n) AS count
    ORDER BY label
    """
    nodes = conn.execute_query(node_query)
    
    rel_query = """
    MATCH ()-[r]->()
    RETURN type(r) AS type, count(r) AS count
    ORDER BY type
    """
    rels = conn.execute_query(rel_query)
    
    return {
        "nodes": {row["label"]: row["count"] for row in nodes},
        "relationships": {row["type"]: row["count"] for row in rels},
        "total_nodes": sum(row["count"] for row in nodes),
        "total_relationships": sum(row["count"] for row in rels)
    }


if __name__ == "__main__":
    print(f"Ket noi Neo4j tai {NEO4J_URI}...")
    conn = get_neo4j_connection()
    
    if conn.verify_connectivity():
        print("Ket noi thanh cong!")
        stats = get_graph_stats()
        print(f"\nThong ke Graph:")
        print(f"  Nodes: {stats['total_nodes']}")
        for label, count in stats['nodes'].items():
            print(f"    - {label}: {count}")
        print(f"  Relationships: {stats['total_relationships']}")
        for rel_type, count in stats['relationships'].items():
            print(f"    - {rel_type}: {count}")
    else:
        print("Ket noi that bai!")
    
    close_neo4j_connection()
