import time
import random
import httpx
import sys
from datetime import datetime, timezone
import string

from common.feature_columns import _home_country_for_user

CHANNELS = ["card_present", "card_not_present", "upi", "login"]
CATEGORIES = ["grocery", "electronics", "travel", "restaurant", "fuel"]
COUNTRIES = ["IN", "US", "GB", "DE", "SG"]
INDIAN_NAMES = [
    "Aarav", "Kabir", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", 
    "Krishna", "Ishaan", "Shaurya", "Atharva", "Ananya", "Diya", "Pihu", "Aaradhya", 
    "Ira", "Sana", "Fatima", "Priya", "Pooja", "Amit", "Rahul", "Rohit", "Vikram", 
    "Sanjay", "Rajesh", "Anil", "Sunita", "Anita", "Geeta", "Neha", "Rohan", "Siddharth",
    "Deepak", "Sandhya", "Karan", "Kunal", "Meera", "Rani", "Kiran", "Vijay"
]

# Seed to match the producer's pool
random.seed(42)
USER_POOL = [f"{name}_{i:03d}" for i, name in enumerate(random.choices(INDIAN_NAMES, k=200))]
random.seed()  # Re-seed with system entropy so live traffic is truly random

def _rand_id(prefix: str, n: int = 8) -> str:
    return f"{prefix}_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

# Create a dictionary to store static profile details for each user
USER_PROFILES = {}
for uid in USER_POOL:
    USER_PROFILES[uid] = {
        "home_country": _home_country_for_user(uid),
        "device_id": _rand_id("dev", 6),
        "typical_amount": random.uniform(10, 300)
    }

def make_transaction() -> dict:
    user_id = random.choice(USER_POOL)
    profile = USER_PROFILES[user_id]
    
    # 95% of the time generate normal transactions, 5% of the time simulate fraud
    is_fraud = random.random() < 0.05
    
    if is_fraud:
        other_countries = [c for c in COUNTRIES if c != profile["home_country"]]
        ip_country = random.choice(other_countries) if other_countries else profile["home_country"]
        device_id = _rand_id("dev", 6)
        amount = round(profile["typical_amount"] * random.uniform(5, 20), 2)
    else:
        ip_country = profile["home_country"]
        device_id = profile["device_id"]
        amount = round(max(1.0, random.gauss(profile["typical_amount"], profile["typical_amount"] * 0.25)), 2)

    return {
        "txn_id": _rand_id("txn"),
        "user_id": user_id,
        "amount": amount,
        "currency": "INR",
        "merchant_id": _rand_id("mer", 5),
        "merchant_category": random.choice(CATEGORIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": device_id,
        "ip_country": ip_country,
        "channel": random.choice(CHANNELS),
    }

def main():
    url = "http://localhost:8000/score"
    print(f"Starting live traffic simulator calling {url}...")
    print("Press Ctrl+C to stop.")
    
    with httpx.Client() as client:
        while True:
            payload = make_transaction()
            try:
                resp = client.post(url, json=payload, timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    prob = data.get("fraud_probability", 0)
                    flagged = data.get("is_flagged", False)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Txn: {payload['txn_id']} | User: {payload['user_id']} | Amount: ${payload['amount']} | Prob: {prob*100:.1f}% | Flagged: {flagged}")
                else:
                    print(f"Error {resp.status_code}: {resp.text}")
            except Exception as e:
                print(f"Request failed: {e}")
            
            # Sleep between 0.5 to 2.0 seconds to simulate real-time traffic
            time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    main()
