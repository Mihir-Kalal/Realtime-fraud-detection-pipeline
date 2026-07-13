import os
import logging
from neo4j import GraphDatabase
from common.schemas import Transaction

logger = logging.getLogger("graph_features.writer")

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "fraudpass")

class GraphWriter:
    def __init__(self):
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        except Exception as e:
            logger.warning("Could not connect to Neo4j: %s", e)
        
    def close(self):
        if self.driver:
            self.driver.close()
        
    def write(self, txn: Transaction):
        if not self.driver:
            return
            
        query = """
        MERGE (u:User {id: $user_id})
        MERGE (m:Merchant {id: $merchant_id})
        MERGE (d:Device {id: $device_id})
        MERGE (c:IPCountry {id: $ip_country})
        
        MERGE (u)-[:TRANSACTED_AT {txn_id: $txn_id, amount: $amount, timestamp: $timestamp}]->(m)
        MERGE (u)-[:USED_DEVICE]->(d)
        MERGE (u)-[:FROM_COUNTRY]->(c)
        """
        try:
            with self.driver.session() as session:
                session.run(query, 
                    user_id=txn.user_id,
                    merchant_id=txn.merchant_id,
                    device_id=txn.device_id,
                    ip_country=txn.ip_country,
                    txn_id=txn.txn_id,
                    amount=txn.amount,
                    timestamp=txn.timestamp.isoformat()
                )
        except Exception as e:
            logger.error("Failed to write to Neo4j for txn_id=%s: %s", txn.txn_id, e)

graph_writer = GraphWriter()
