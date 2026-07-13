import logging
from graph_features.writer import graph_writer

logger = logging.getLogger("graph_features.queries")

def get_shared_device_count(user_id: str) -> int:
    if not graph_writer.driver:
        return 0
    query = """
    MATCH (u:User {id: $user_id})-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(other:User)
    RETURN count(DISTINCT other) as shared_count
    """
    try:
        with graph_writer.driver.session() as session:
            result = session.run(query, user_id=user_id)
            record = result.single()
            return record["shared_count"] if record else 0
    except Exception as e:
        logger.error("Failed to query shared devices for %s: %s", user_id, e)
        return 0

def get_shared_merchant_fraud_count(merchant_id: str) -> int:
    """Count distinct users flagged as fraud who also transacted at this merchant."""
    if not graph_writer.driver:
        return 0
    query = """
    MATCH (u:User)-[:TRANSACTED_AT]->(m:Merchant {id: $merchant_id})
    WHERE u:FlaggedFraud
    RETURN count(DISTINCT u) as fraud_user_count
    """
    try:
        with graph_writer.driver.session() as session:
            result = session.run(query, merchant_id=merchant_id)
            record = result.single()
            return record["fraud_user_count"] if record else 0
    except Exception as e:
        logger.error("Failed to query merchant fraud count for %s: %s", merchant_id, e)
        return 0

def get_hop_distance_to_known_fraud(user_id: str) -> int:
    """Shortest path (up to 5 hops) from this user to any FlaggedFraud user
    through shared devices or merchants. Returns 0 if no path or Neo4j is down."""
    if not graph_writer.driver:
        return 0
    query = """
    MATCH (start:User {id: $user_id}),
          (fraud:User:FlaggedFraud)
    WHERE start <> fraud
    MATCH p = shortestPath((start)-[*..5]-(fraud))
    RETURN length(p) AS hops
    ORDER BY hops ASC
    LIMIT 1
    """
    try:
        with graph_writer.driver.session() as session:
            result = session.run(query, user_id=user_id)
            record = result.single()
            return record["hops"] if record else 0
    except Exception as e:
        logger.error("Failed to query hop distance for %s: %s", user_id, e)
        return 0
