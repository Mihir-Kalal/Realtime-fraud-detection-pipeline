import random
import uuid
from datetime import datetime, timezone
from locust import HttpUser, task, between

class FraudScoringUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def score_transaction(self):
        txn_id = f"txn_{uuid.uuid4().hex}"
        user_id = f"user_{random.randint(1, 1000):04d}"
        payload = {
            "txn_id": txn_id,
            "user_id": user_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "currency": "INR",
            "merchant_id": f"merchant_{random.randint(1, 100):03d}",
            "merchant_category": random.choice(["electronics", "groceries", "travel"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_id": f"device_{uuid.uuid4().hex[:8]}",
            "ip_country": "IN",
            "channel": "card_present"
        }
        
        with self.client.post("/score", json=payload, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed! Status code: {response.status_code}")
