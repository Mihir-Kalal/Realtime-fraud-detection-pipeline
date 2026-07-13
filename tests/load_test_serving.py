"""
Simple async load test for fraud-scoring-api's POST /score.

Prints both client-observed latency (network + queueing + server processing)
and the API's reported `latency_ms` (feature fetch + inference + SHAP).

Usage:
    PYTHONPATH=. pip install httpx numpy
    python tests/load_test_serving.py --url http://localhost:8000/score \
        --requests 500 --concurrency 20

Exits non-zero if the selected p99 SLA metric exceeds --sla-ms.
"""
import argparse
import asyncio
import random
import statistics
import string
import sys
import time
from datetime import datetime, timezone

import httpx
import numpy as np

CHANNELS = ["card_present", "card_not_present", "upi", "login"]
CATEGORIES = ["grocery", "electronics", "travel", "restaurant", "fuel"]
COUNTRIES = ["US", "GB", "IN", "DE", "BR", "NG"]
USER_POOL = [f"user_{i:05d}" for i in range(200)]


def _rand_id(prefix: str, n: int = 8) -> str:
    return f"{prefix}_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


# Create a dictionary to store static profile details for each user
USER_PROFILES = {}
for uid in USER_POOL:
    USER_PROFILES[uid] = {
        "home_country": random.choice(COUNTRIES),
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
        "currency": "USD",
        "merchant_id": _rand_id("mer", 5),
        "merchant_category": random.choice(CATEGORIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": device_id,
        "ip_country": ip_country,
        "channel": random.choice(CHANNELS),
    }


async def worker(
    client: httpx.AsyncClient,
    url: str,
    n_requests: int,
    latencies_ms: list[float],
    server_latencies_ms: list[float],
    errors: list[str],
):
    for _ in range(n_requests):
        payload = make_transaction()
        t0 = time.perf_counter()
        try:
            resp = await client.post(url, json=payload, timeout=5.0)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if resp.status_code != 200:
                errors.append(f"HTTP {resp.status_code}: {resp.text[:200]}")
                continue
            latencies_ms.append(elapsed_ms)
            body = resp.json()
            if "latency_ms" in body:
                server_latencies_ms.append(float(body["latency_ms"]))
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))


def percentile(data: list[float], p: float) -> float:
    if not data:
        return float("nan")
    return float(np.percentile(data, p))


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000/score")
    parser.add_argument("--requests", type=int, default=500, help="total requests")
    parser.add_argument("--concurrency", type=int, default=20, help="concurrent workers")
    parser.add_argument("--sla-ms", type=float, default=100.0)
    parser.add_argument(
        "--sla-metric",
        choices=("server", "client"),
        default="server",
        help="which p99 latency to use for pass/fail",
    )
    args = parser.parse_args()

    per_worker = args.requests // args.concurrency
    remainder = args.requests % args.concurrency
    counts = [per_worker + (1 if i < remainder else 0) for i in range(args.concurrency)]

    latencies_ms: list[float] = []
    server_latencies_ms: list[float] = []
    errors: list[str] = []

    limits = httpx.Limits(
        max_connections=args.concurrency * 2,
        max_keepalive_connections=args.concurrency,
    )
    async with httpx.AsyncClient(limits=limits) as client:
        # Warm-up requests, excluded from measurement — first-hit JIT/connection
        # setup isn't representative of steady-state p99.
        for _ in range(min(10, args.concurrency)):
            try:
                await client.post(args.url, json=make_transaction(), timeout=5.0)
            except Exception:  # noqa: BLE001
                pass

        start = time.perf_counter()
        await asyncio.gather(
            *[
                worker(client, args.url, n, latencies_ms, server_latencies_ms, errors)
                for n in counts
            ]
        )
        wall_s = time.perf_counter() - start

    ok = len(latencies_ms)
    total = ok + len(errors)
    print(f"\n=== Load test results: {args.url} ===")
    print(f"requests: {total}  ok: {ok}  errors: {len(errors)}  wall_time_s: {wall_s:.2f}")
    if total:
        print(f"throughput_rps: {total / wall_s:.1f}")
    if errors:
        print(f"sample error: {errors[0]}")

    if latencies_ms:
        print("\n-- client-observed latency (includes HTTP overhead) --")
        print(f"  p50: {percentile(latencies_ms, 50):.2f} ms")
        print(f"  p95: {percentile(latencies_ms, 95):.2f} ms")
        print(f"  p99: {percentile(latencies_ms, 99):.2f} ms")
        print(f"  max: {max(latencies_ms):.2f} ms")
        print(f"  mean: {statistics.mean(latencies_ms):.2f} ms")

    if server_latencies_ms:
        print("\n-- server-reported latency_ms (feature fetch + inference + SHAP only) --")
        print(f"  p50: {percentile(server_latencies_ms, 50):.2f} ms")
        print(f"  p95: {percentile(server_latencies_ms, 95):.2f} ms")
        print(f"  p99: {percentile(server_latencies_ms, 99):.2f} ms")

    client_p99 = percentile(latencies_ms, 99) if latencies_ms else float("inf")
    server_p99 = percentile(server_latencies_ms, 99) if server_latencies_ms else float("inf")
    sla_p99 = server_p99 if args.sla_metric == "server" else client_p99
    passed = sla_p99 <= args.sla_ms
    print(
        f"\nSLA check: {args.sla_metric} p99 {sla_p99:.2f} ms <= "
        f"{args.sla_ms} ms -> {'PASS' if passed else 'FAIL'}"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
