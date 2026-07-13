import redis
import random

# Match these exactly to common/feature_columns.py
FEATURE_COLUMNS = [
    "txn_velocity_1h",
    "txn_velocity_24h",
    "amount_mean_24h",
    "amount_std_24h",
    "amount_zscore",
    "distinct_merchants_1h",
    "distinct_merchants_24h",
    "impossible_travel_flag",
    "seconds_since_last_txn",
    "shared_device_count",
    "shared_merchant_fraud_count",
    "hop_distance_to_fraud"
]

def main():
    print("Connecting to Redis...")
    # This will work when run from the host machine or any container connected to the network
    # We use 'fraud-redis' if running inside docker, 'localhost' if running on host.
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        r.ping()
    except:
        r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    
    # We will generate 1000 users to match Locust's range
    num_users = 1000
    fraud_rate = 0.05
    
    fraud_count = 0
    safe_count = 0
    
    for i in range(1, num_users + 1):
        user_id = f"user_{i:04d}"
        key = f"features:user:{user_id}"
        
        is_fraud = random.random() < fraud_rate
        
        features = {}
        if is_fraud:
            # Fraudulent behavior profile
            features = {
                "txn_velocity_1h": random.uniform(10, 50),
                "txn_velocity_24h": random.uniform(50, 200),
                "amount_mean_24h": random.uniform(500, 2000),
                "amount_std_24h": random.uniform(100, 500),
                "amount_zscore": random.uniform(3.0, 10.0),
                "distinct_merchants_1h": random.uniform(5, 15),
                "distinct_merchants_24h": random.uniform(10, 30),
                "impossible_travel_flag": 1.0, # True
                "seconds_since_last_txn": random.uniform(1, 60),
                "shared_device_count": random.uniform(2, 5),
                "shared_merchant_fraud_count": random.uniform(1, 10),
                "hop_distance_to_fraud": 1.0
            }
            fraud_count += 1
        else:
            # Safe behavior profile
            features = {
                "txn_velocity_1h": random.uniform(0, 2),
                "txn_velocity_24h": random.uniform(1, 5),
                "amount_mean_24h": random.uniform(20, 100),
                "amount_std_24h": random.uniform(5, 20),
                "amount_zscore": random.uniform(-1.0, 1.0),
                "distinct_merchants_1h": random.uniform(1, 2),
                "distinct_merchants_24h": random.uniform(1, 4),
                "impossible_travel_flag": 0.0,
                "seconds_since_last_txn": random.uniform(3600, 86400),
                "shared_device_count": 0.0,
                "shared_merchant_fraud_count": 0.0,
                "hop_distance_to_fraud": random.uniform(3.0, 5.0)
            }
            safe_count += 1
            
        r.hset(key, mapping=features)
        
    print(f"Successfully loaded {num_users} users into Redis!")
    print(f"Generated {safe_count} safe profiles and {fraud_count} fraud profiles (~{fraud_rate*100}% fraud rate).")

if __name__ == "__main__":
    main()
