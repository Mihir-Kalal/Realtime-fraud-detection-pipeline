# 🚀 Enterprise Real-Time Fraud Detection Pipeline

![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104-009688.svg?logo=fastapi)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.6-black.svg?logo=apachekafka)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D.svg?logo=redis)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg?logo=postgresql)
![MLflow](https://img.shields.io/badge/MLflow-2.14-0194E2.svg?logo=mlflow)
![ONNX](https://img.shields.io/badge/ONNX_Runtime-1.16-005CED.svg)

An end-to-end, production-grade Machine Learning pipeline designed for **extreme high-throughput, low-latency financial transaction scoring**. 

This system acts as a complete MLOps ecosystem capable of ingesting streaming transaction data, computing graph-based features in real-time, scoring transactions via a Multi-Armed Bandit, and autonomously retraining models upon detecting data drift. **It is highly optimized to sustain >3,100 requests per second (31,900+ predictions per 10s) while maintaining an average latency of 3.9ms and strict sub-7ms p99 latencies.**

## 🏗 System Architecture

```mermaid
graph TD
    %% Define Styles
    classDef stream fill:#f9f,stroke:#333,stroke-width:2px;
    classDef db fill:#bbf,stroke:#333,stroke-width:2px;
    classDef compute fill:#dfd,stroke:#333,stroke-width:2px;
    classDef client fill:#fbb,stroke:#333,stroke-width:2px;
    classDef model fill:#ff9,stroke:#333,stroke-width:2px;

    Client([User Transactions]) -->|HTTP POST| API(FastAPI Serving API)
    
    %% Ingestion Flow
    Producer(Data Producer) -->|Streams| Kafka(Apache Kafka)
    Kafka -->|Consumes| Flink(Feature Engine)
    
    %% Feature Engine Storage
    Flink -->|Graph Traversals| Neo4j[(Neo4j Graph DB)]
    Flink -->|Cold Storage / Logs| Postgres[(PostgreSQL)]
    Flink -->|Hot Features| Redis[(Redis Feature Store)]
    
    %% Serving Flow
    API -.->|1. In-Process Cache| Cache[TTLCache]
    API -->|2. Fetch Features < 1ms| Redis
    Redis -.->|Circuit Breaker Fallback| API
    API -->|3. Thompson Sampling| MAB{Top 2 Models}
    MAB -->|4. Micro-Batch Inference| ONNX(ONNX Runtime)
    API -->|5. Async Logging| Postgres
    API -->|6. Async Explainability| SHAP(SHAP Explainer)
    SHAP -->|Logs Explanations| Postgres
    
    %% MLOps Flow
    Feedback(Delayed Feedback Loop) -->|Writes True Labels| Postgres
    Postgres -.->|Training Data| Training(Automated Training)
    Training -->|Logs Artifacts| MLflow(MLflow Registry)
    MLflow -.->|Hot-swaps Top 2| API
    
    %% Monitoring
    API -->|Live Inference Data| Monitoring(Drift Monitor)
    Postgres -->|Baseline Data| Monitoring
    Monitoring -->|Triggers Retraining| Training
    Monitoring -->|Metrics| Dashboard[Streamlit Dashboard]

    %% Assign Classes
    class Client,Dashboard client;
    class Kafka stream;
    class Neo4j,Postgres,Redis,MLflow db;
    class API,Producer,Flink,Cache,MAB,SHAP,Feedback,Training,Monitoring compute;
    class ONNX model;
```

The pipeline is fully containerized via Docker Compose and microservice-oriented:

* 📥 **Data Ingestion (Kafka):** Simulates a high-throughput stream of synthetic transactions and acts as an event-driven buffer.
* ⚙️ **Streaming Feature Engine (Flink/Python):** Consumes raw transactions, computes real-time sliding-window aggregations, and extracts multi-hop features from **Neo4j**.
* 🗄️ **Dual-Layer Feature Store:** 
  * **Redis:** Ultra-low latency hot cache for real-time inference (`O(1)` lookups).
  * **Postgres:** Historical system of record for transaction logs, delayed labels, and offline training data.
* 🧠 **Serving API (FastAPI + Gunicorn):** A heavily optimized, asynchronous scoring API executing **ONNX** models. Features in-process caching, dynamic micro-batching, and Redis Circuit Breakers.
* 🧪 **Automated MLOps (MLflow):** 
  * **Multi-Armed Bandit:** Dynamically routes live traffic between the Top 2 historical models using Bayesian Thompson Sampling.
  * **Drift Detection:** Continuously calculates Population Stability Index (PSI) to autonomously trigger background retraining when data distribution shifts.
* 📊 **Live Dashboard (Streamlit):** Web UI for real-time visualization of transaction volumes, fraud rates, and model drift metrics.

## 🚀 Getting Started

### Prerequisites
*   Docker & Docker Compose
*   Make sure ports `8000`, `8501`, `5432`, `6379`, `9092`, `5000`, and `7474` are available.

### Running the Pipeline

1. **Start the Infrastructure:**
   Spin up the entire architecture using Docker Compose:
   ```bash
   docker-compose up -d
   ```

2. **Access the UIs:**
   * **Streamlit Monitoring Dashboard:** [http://localhost:8501](http://localhost:8501)
   * **MLflow Model Registry:** [http://localhost:5000](http://localhost:5000)
   * **Neo4j Browser:** [http://localhost:7474](http://localhost:7474) (Default auth: `neo4j` / `fraudpass`)

3. **Interact with the API / Simulate Transactions:**
   The Serving API is available at `http://localhost:8000`. You can run the live simulator script to automatically generate and score transaction traffic:
   ```bash
   PYTHONPATH=. venv/bin/python tests/live_simulator.py
   ```

   Here is a preview of the transaction generator simulator outputting and scoring live transaction streams:
   
   ![Transaction Generation Simulator](assets/simulator_screenshot.png)

   You can also run a high-load scenario using the provided load tests:
   ```bash
   cd tests/load
   ./run_load_test.sh
   ```

## 🧠 Feature Engineering (Fraud Signals)

The Machine Learning model scores transactions based on highly complex, real-time streaming aggregations and graph traversals. The `feature_engine` dynamically extracts the following fraud signals on the fly:

*   **Velocity Rules:** `txn_velocity_1h`, `txn_velocity_24h` (Detects brute-force card testing).
*   **Amount Anomalies:** `amount_zscore` (Identifies transactions that deviate heavily from a user's 24-hour historical rolling average).
*   **Merchant Diversity:** `distinct_merchants_1h`, `distinct_merchants_24h` (Detects if a stolen card is being used across multiple vendors rapidly).
*   **Impossible Travel:** `impossible_travel_flag` (Flags if a user's IP country and Device ID abruptly change within a physically impossible timeframe).
*   **Graph Network Features (Neo4j):**
    *   `shared_device_count`: How many different users are transacting from this exact device?
    *   `shared_merchant_fraud_count`: Is this merchant a known hotspot for stolen cards?
    *   `hop_distance_to_fraud`: How many degrees of separation is this user from a known fraudster in the Neo4j graph?

## ⚡ Architecture & Optimization Timeline

To achieve both `< 5ms` latency under extreme load and true enterprise-grade reliability, the pipeline's architecture evolved through several optimization phases. Here is the chronological implementation of our optimizations and why they were chosen over the basic approaches:

### Phase 1: Ingestion & Processing Backbone
1. **Event-Driven Backbone (Kafka)**
   * **Basic Choice:** Point-to-point HTTP communication between the producer and the feature engine.
   * **Advanced Choice:** Using Apache Kafka as a central message broker.
   * **Why we did it:** Kafka decouples producers from consumers. It acts as a massive buffer that prevents the pipeline from crashing during sudden traffic spikes, and allows for seamless horizontal scaling of feature engine consumers.

2. **Stream Aggregations (Flink/Python)**
   * **Basic Choice:** Calculating heavy sliding-window features on the fly during the API request.
   * **Advanced Choice:** Using Flink/Python to pre-compute heavy aggregations in the background, writing the final values to Redis.
   * **Why we did it:** Doing heavy math on the fly spikes API latency. Pre-computing features ensures the scoring API only has to perform an `O(1)` cache lookup.

3. **Graph Traversals with Neo4j**
   * **Basic Choice:** Using massive `JOIN` operations in Postgres to find complex fraud rings (e.g., users sharing IPs).
   * **Advanced Choice:** Offloading relationship mapping to a dedicated graph database (Neo4j).
   * **Why we did it:** Relational databases are terrible at multi-hop relationship queries. Neo4j can traverse graph relationships in milliseconds, allowing for real-time graph features.

### Phase 2: Core Serving & Reliability
4. **High-Performance Drivers (asyncpg)**
   * **Basic Choice:** Standard blocking Python database drivers (`psycopg2`).
   * **Advanced Choice:** Uses `asyncpg` (written in Cython) for all database interactions.
   * **Why we did it:** To ensure lightning-fast, non-blocking Postgres queries that do not freeze the asyncio event loop.

5. **Zero-Downtime Hot-Reloading**
   * **Basic Choice:** Restarting the API container every time a new model is deployed.
   * **Advanced Choice:** A background polling loop that checks the Model Registry and hot-swaps ONNX models in memory without dropping a single active request.
   * **Why we did it:** Restarting containers causes downtime. Hot-reloading ensures true 24/7 continuous availability, which is critical for financial transaction processing.

6. **Circuit Breakers (Redis)**
   * **Basic Choice:** Allowing the API to crash if the Redis cache becomes unresponsive.
   * **Advanced Choice:** Wrapping Redis calls in a Circuit Breaker that gracefully falls back to a "cold-start" (all-zeros feature vector).
   * **Why we did it:** High availability is better than perfect accuracy. It is better to score a transaction using a default vector than to fail the HTTP request entirely during a temporary Redis outage.

### Phase 3: Advanced MLOps Patterns
7. **Asynchronous Explainability (SHAP)**
   * **Basic Choice:** Calculating SHAP feature importance synchronously before returning the API response.
   * **Advanced Choice:** Fully offloading SHAP calculations to a ThreadPool via FastAPI BackgroundTasks.
   * **Why we did it:** SHAP calculations are highly CPU-intensive and can take up to 20ms. Running them in the background ensures explainability never blocks the API's `O(ms)` response time for the end user.

8. **Multi-Armed Bandit (Thompson Sampling)**
   * **Basic Choice:** Running a static 50/50 A/B test for model evaluation.
   * **Advanced Choice:** Implementing Bayesian Thompson Sampling to dynamically monitor precision/recall and route more traffic to the best-performing model over time.
   * **Why we did it:** A static 50/50 split causes business loss if the new model performs poorly. Thompson Sampling automatically shifts traffic to the winner in real-time, minimizing revenue loss while still exploring safely.

9. **Automated Drift Detection**
   * **Basic Choice:** Manually running Jupyter notebooks once a month to check if the model is still accurate.
   * **Advanced Choice:** Continuously calculating Population Stability Index (PSI) between live inference data and the baseline training data, automatically triggering a retrain if data drift exceeds safe thresholds.
   * **Why we did it:** Fraud patterns shift rapidly. Automated drift detection ensures the model is always operating on the most up-to-date data distributions without manual intervention.

10. **Top-K Candidate Pool & Dynamic Hot-Swapping**
    * **Basic Choice:** Loading the newest model into RAM, completely abandoning older models.
    * **Advanced Choice:** The background training script maintains a Candidate Pool of the Top 3 historical models + 1 new model. The Serving API polls the database every 30s, ranks the 4 candidates by AUC, and dynamically hot-swaps only the absolute Top 2 models into RAM without dropping traffic.
    * **Why we did it:** This prevents "Model Sprawl" by strictly limiting active models to 4, preventing RAM exhaustion. Furthermore, it mathematically guarantees that the live API is protected by a safety gate: if a newly trained model has a poor AUC, the API ignores it and continues serving the historical Top 2.

### Phase 4: High-Frequency Trading (HFT) Extreme Scaling
11. **Multi-Worker Scaling (Gunicorn & Uvicorn)**
    * **Basic Choice:** Running FastAPI using a single Uvicorn process.
    * **Advanced Choice:** Migrating to Gunicorn with 4 `UvicornWorker` processes.
    * **Why we did it:** A single process maxes out exactly one CPU core, capping throughput. By spawning 4 Gunicorn workers, the OS can distribute incoming HTTP traffic evenly across multiple CPU cores, effectively quadrupling maximum throughput and preventing dropped connections under heavy load.

12. **Dynamic Micro-Batching (ONNX Runtime)**
    * **Basic Choice:** Executing ONNX inference sequentially for every single incoming request.
    * **Advanced Choice:** An `asyncio`-based Micro-Batcher that queues incoming concurrent requests for a maximum of 5ms and groups them into contiguous batches (up to 50 vectors).
    * **Why we did it:** Individual inference calls suffer from severe thread contention and Python overhead. Batching array operations before passing them to the C++ ONNX engine unlocks SIMD vectorization and drastically reduces CPU overhead.

13. **In-Process Feature Caching**
    * **Basic Choice:** Querying the Redis Feature Store over TCP for every transaction.
    * **Advanced Choice:** Implementing an in-process `TTLCache` (Max size: 100,000 users, TTL: 10s) directly in the API memory space.
    * **Why we did it:** Network I/O, even to localhost Redis, adds ~1-2ms of latency. By caching recent feature vectors in Python RAM, follow-up transactions from the same user are served in `O(1)` time, completely bypassing the network.

14. **Thundering Herd Protection (Redis Connection Tuning)**
    * **Basic Choice:** Using default Redis connection pool limits (e.g., 50 connections).
    * **Advanced Choice:** Massively expanding `REDIS_MAX_CONNECTIONS` to 1,000.
    * **Why we did it:** When a load test first starts, thousands of concurrent requests miss the empty TTLCache and hit Redis simultaneously (a "Thundering Herd"). A small connection pool instantly exhausts, stalling the asyncio event loop and spiking p99 latency. Expanding the pool absorbs this initial burst seamlessly until the in-memory cache warms up.

15. **Async Database Logging & Connection Tuning**
    * **Basic Choice:** Awaiting Postgres `INSERT` statements before returning the API response, with default pool sizes.
    * **Advanced Choice:** Moving database inserts to FastAPI `BackgroundTasks`, and strictly tuning the `asyncpg` connection pool `MAX_SIZE` down to 10 per worker (40 total).
    * **Why we did it:** Database I/O blocks the hot path. Moving it to the background allows the API to return the prediction instantly. Tuning the connection pool prevents "Connection Thrashing", ensuring the database remains highly responsive.

16. **JSON Serialization (ORJSON)**
    * **Basic Choice:** Using Python's standard library `json` module (the FastAPI default).
    * **Advanced Choice:** Replacing FastAPI's default response class with `ORJSONResponse`.
    * **Why we did it:** Standard JSON serialization is written in Python and consumes valuable CPU cycles. `orjson` is a highly optimized, Rust-based library that significantly speeds up JSON encoding, shaving off crucial microseconds on the hot path.

## 📂 Project Structure

* 🚀 `/producer` - Generates synthetic streaming transactions.
* ⚙️ `/feature_engine` - Computes features and writes to Redis/Postgres.
* ⚡ `/serving` - The low-latency model inference API (FastAPI).
* 🧠 `/training` - Model training and ONNX export logic.
* 📊 `/monitoring` - Data drift detection and Streamlit dashboard.
* 🔁 `/feedback` - Simulates delayed fraud labels.
* 🕸️ `/graph_features` - Neo4j queries for graph-based fraud signals.
* 🌊 `/flink_streaming` - Flink jobs for heavy stream aggregations.
* 🧪 `/tests` - Unit, integration, and Locust load testing scripts.
* 🌐 `/frontend` - Static UI files.
