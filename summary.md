# 🏛️ END-TO-END SYSTEM SUMMARY: Real-Time Fraud Detection Pipeline (Modules 1–9)
## 💳 Transaction Ka Safar — Swipe Se Lekar Fraud Decision, MLOps Retraining Aur Production Resiliency Tak

> **Language:** Hinglish (Conversational Technical Hindi + English)  
> **Coverage:** Module 1 se lekar Module 9 tak ka complete deep dive  
> **Core Constraint:** Sub-7ms P99 Latency SLA & >3,100 RPS Sustained Throughput  
> **Target Audience:** System Design & ML Engineering Interviews (Senior / Staff level preparation)

---

## 📑 TABLE OF CONTENTS (Index)

1. [Executive Overview: System Ka 10,000 Feet View](#1-executive-overview-system-ka-10000-feet-view)
2. [Master Architecture Diagram: End-to-End Pipeline](#2-master-architecture-diagram-end-to-end-pipeline)
3. [Transaction Ka Complete Safar (Step-by-Step Microscopic Journey)](#3-transaction-ka-complete-safar-step-by-step-microscopic-journey)
4. [Module-by-Module Comprehensive Breakdown (Modules 1 to 9)](#4-module-by-module-comprehensive-breakdown-modules-1-to-9)
   - [Module 1: Concurrency, Asyncio & Docker Infrastructure](#module-1-python-concurrency-asyncio--docker-infrastructure)
   - [Module 2: Distributed Message Streaming (Apache Kafka KRaft)](#module-2-distributed-message-streaming-apache-kafka-kraft)
   - [Module 3: Real-Time Feature Store (Redis 7.2 & Apache Flink)](#module-3-real-time-feature-store-redis-72--apache-flink)
   - [Module 4: Multi-Tier Storage & Graph Analytics (Neo4j & Postgres)](#module-4-multi-tier-storage--graph-analytics-neo4j--postgres)
   - [Module 5: Ultra-Low Latency Serving & Inference (FastAPI & ONNX Runtime)](#module-5-ultra-low-latency-serving--inference-fastapi--onnx-runtime)
   - [Module 6: Fraud Machine Learning Mathematics & Decision Theory](#module-6-fraud-machine-learning-mathematics--decision-theory)
   - [Module 7: Dynamic Model Routing & Bayesian Bandits](#module-7-dynamic-model-routing--bayesian-bandits)
   - [Module 8: Continuous MLOps, Drift Detection & Feedback Loops](#module-8-continuous-mlops-drift-detection--feedback-loops)
   - [Module 9: Production Resiliency & Performance Benchmarking](#module-9-production-resiliency--performance-benchmarking)
5. [5 Transaction Archetypes Ka Real-World Safar (Scenarios)](#5-5-transaction-archetypes-ka-real-world-safar-scenarios)
6. [Latency Budget Breakdown (Har Ek Millisecond Ka Hisaab)](#6-latency-budget-breakdown-har-ek-millisecond-ka-hisaab)
7. [Glossary of Complex Technical Terms (Hinglish Demystified)](#7-glossary-of-complex-technical-terms-hinglish-demystified)
8. [Master Interview Cheatsheet (Rapid Verbal Defense)](#8-master-interview-cheatsheet-rapid-verbal-defense)

---

## 1. Executive Overview: System Ka 10,000 Feet View

### 🎯 Yeh System Kyu Banaya Gaya Hai?
Imagine karo jab tum Swiggy, Amazon ya kisi POS machine par apna card swipe karte ho ya UPI pin daalte ho. Screen par loading wheel ghumta hai. Bank ke paas sirf **kuch milliseconds** hote hain yeh decide karne ke liye ki:
1. Yeh transaction **genuine** hai (cardholder khud khareed raha hai)?
2. Ya fir kisi hacker ne card details chori karke **fraudulent transaction** trigger kiya hai?

Agar tumhara system **7 milliseconds** se zyada time lega, toh payment gateway timeout ho jayega aur customer ka checkout experience kharab hoga. Aur agar tumne galat decision liya:
- **False Positive (Galti se legitimate user ko block kiya):** Genuine customer naraz hoga, checkout chhod dega, aur bank ki brand reputation kharab hogi.
- **False Negative (Fraud transaction ko nikalne diya):** Bank aur merchant ko direct financial loss hoga (chargebacks, penalties, fraud loss).

Hamara system ek **ultra-low-latency, distributed, self-healing real-time fraud detection pipeline** hai jo:
- **>3,100 requests per second (RPS)** ka load bina kisi hitch ke handle karta hai.
- **Average Latency ~3.9ms** aur **P99 Tail Latency < 7ms** deliver karta hai.
- Real-time streaming (Kafka), in-memory hot feature store (Redis), graph database (Neo4j for mule rings), high-performance C++ ML inference (ONNX Runtime), Bayesian Multi-Armed Bandits, automated drift detection, aur production resilience (Circuit Breakers) ko ek seamless loop me integrate karta hai.

---

## 2. Master Architecture Diagram: End-to-End Pipeline

Neeche diya gaya Mermaid diagram dikhata hai ki kaise ek transaction entry point se lekar multiple tiers se guzarti hai:

```mermaid
flowchart TD
    %% Global Styling
    classDef client fill:#E1F5FE,stroke:#0288D1,stroke-width:2px;
    classDef stream fill:#FFF3E0,stroke:#F57C00,stroke-width:2px;
    classDef storage fill:#E8F5E9,stroke:#388E3C,stroke-width:2px;
    classDef compute fill:#EDE7F6,stroke:#512DA8,stroke-width:2px;
    classDef decision fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px;
    classDef approve fill:#E8F8F5,stroke:#00B894,stroke-width:2px;
    classDef decline fill:#FFEBEE,stroke:#D32F2F,stroke-width:2px;
    classDef stepup fill:#FFF8E1,stroke:#FFA000,stroke-width:2px;
    classDef async fill:#ECEFF1,stroke:#607D8B,stroke-width:1.5px,stroke-dasharray: 4 4;

    subgraph INGRESS ["1. Ingress & Traffic Generation (Mod 1 & 2)"]
        CLI["Payment Gateway / Checkout Client"]:::client
        PROD["Synthetic Producer (10k txns/batch)"]:::compute
        GUNI["Gunicorn Master (SO_REUSEPORT)"]:::compute
        W1["Worker 1 (uvloop)"]:::compute
        W2["Worker 2 (uvloop)"]:::compute
        W3["Worker 3 (uvloop)"]:::compute
        W4["Worker 4 (uvloop)"]:::compute
    end

    subgraph KAFKA ["2. Distributed Streaming - Kafka KRaft (Mod 2)"]
        KP0[["Partition 0: Normal"]]:::stream
        KP1[["Partition 1: Bursts"]]:::stream
        KP2[["Partition 2: Rings"]]:::stream
        KDLQ[["Topic: transactions:dlq"]]:::decline
    end

    subgraph FEAT_ENG ["3. Real-Time Feature Engine (Mod 3 & 4)"]
        K_CONS["KafkaReader (Manual Commit)"]:::compute
        ZSET[("Redis ZSET: Sliding Window (24h)")]:::storage
        HASH_USER[("Redis HASH: features:user:{id}")]:::storage
        FLINK["Flink Stream Scale-Up Blueprint"]:::async
    end

    subgraph SERVING ["4. Ultra-Low Latency Serving Tier (<7ms) (Mod 4 & 5)"]
        L1_CACHE[("L1 TTLCache (In-Memory Heap)")]:::storage
        CB{"pybreaker Circuit Breaker"}:::decision
        L2_REDIS[("L2 Redis Hot Store")]:::storage
        BATCHER[["ModelBatcher (Micro-Queue 50txns/5ms)"]]:::stream
        ONNX_CPP["ONNX Runtime C++ Engine (AVX-512)"]:::compute
    end

    subgraph DECISION ["5. Routing & Multi-Tier Decision (Mod 6 & 7)"]
        MAB{"Bayesian Thompson Sampling Router"}:::decision
        CHAMP["Champion Model (v1)"]:::compute
        SHADOW["Shadow Challenger Model (v2)"]:::async
        THRES{"Cost-Sensitive Threshold (F0.5 = 0.78)"}:::decision
        ACT_APP(["TIER 1: AUTO-APPROVE (<0.20)"]):::approve
        ACT_STP(["TIER 2: STEP-UP 3DS (0.20-0.78)"]):::stepup
        ACT_DEC(["TIER 3: HARD DECLINE (>=0.78)"]):::decline
    end

    subgraph POST_PROCESS ["6. Async Audit, Graph & MLOps Feedback (Mod 4, 8 & 9)"]
        SHAP_OFF["TreeSHAP (asyncio.to_thread)"]:::compute
        PG_DB[("PostgreSQL 16: predictions & snapshots")]:::storage
        NEO4J[("Neo4j 5.20: Mule Graph Analytics")]:::storage
        DRIFT_DAEMON["Drift Daemon (PSI & KS-Test)"]:::compute
        MLFLOW["MLflow Registry & Hot-Reload"]:::compute
        DASH["Streamlit Dashboard :8501"]:::client
    end

    %% Connections
    CLI --> GUNI
    GUNI --> W1 & W2 & W3 & W4
    PROD -->|Murmur2(user_id)| KP0 & KP1 & KP2
    PROD -.->|Malformed JSON| KDLQ

    KP0 & KP1 & KP2 --> K_CONS
    K_CONS -->|Pipeline 1: ZREMRANGEBYSCORE + ZRANGE| ZSET
    K_CONS -->|Pipeline 2: ZADD + HSET| HASH_USER

    W1 & W2 & W3 & W4 --> L1_CACHE
    L1_CACHE -->|Cache Miss| CB
    CB -->|Closed| L2_REDIS
    CB -->|Open Fallback| BATCHER
    L2_REDIS --> BATCHER
    L1_CACHE -->|Cache Hit| BATCHER

    BATCHER --> ONNX_CPP
    ONNX_CPP --> MAB
    MAB --> CHAMP
    MAB -.->|Shadow Copy| SHADOW
    CHAMP --> THRES

    THRES -->|p < 0.20| ACT_APP
    THRES -->|0.20 <= p < 0.78| ACT_STP
    THRES -->|p >= 0.78| ACT_DEC

    ACT_APP & ACT_STP & ACT_DEC -->|Async BackgroundTask| PG_DB
    ACT_DEC & ACT_STP -->|is_flagged = True| SHAP_OFF
    SHAP_OFF -->|JSONB Update| PG_DB

    KP0 & KP1 & KP2 -.->|Async Stream| NEO4J
    NEO4J -.->|Graph Signals: hop_distance| HASH_USER

    PG_DB --> DRIFT_DAEMON
    DRIFT_DAEMON -->|Feature Drift PSI >= 0.25| MLFLOW
    MLFLOW -->|Atomic 64-bit Pointer Swap| ONNX_CPP
    PG_DB --> DASH
```

---

## 3. Transaction Ka Complete Safar (Step-by-Step Microscopic Journey)

Jab ek customer "Pay Now" click karta hai, toh transaction ke sath agle **3.9 milliseconds** me kya hota hai? Chalo iska microscopic safar dekhte hain:

```
[Customer Pay]
      │
      ▼ (Step 1: Network Ingress)
Linux Kernel Socket (`SO_REUSEPORT`) -> 4 Gunicorn Workers me se ek choose hota hai
      │
      ▼ (Step 2: Deserialization)
Pydantic v2 (Rust Core) JSON bytes ko parse karta hai (~0.25ms)
      │
      ▼ (Step 3: Multi-Tier Feature Lookup)
L1 Heap Cache (TTLCache) check hoti hai (<0.005ms).
Miss hone par Circuit Breaker se hote hue L2 Redis Hash se 16 features aate hain (~0.5ms)
      │
      ▼ (Step 4: Dynamic Micro-Batching)
ModelBatcher queue me request wait karti hai (max 5ms ya 50 items) -> CPU yield hota hai
      │
      ▼ (Step 5: High-Speed ML Scoring)
ONNX Runtime C++ Engine me batch execute hota hai -> GIL released -> AVX-512 SIMD execution (~1.5ms)
      │
      ▼ (Step 6: Bandit Routing & Decision)
Bayesian Thompson Sampling champion model select karta hai -> Calibrated probability p̂ aati hai
p̂ < 0.20  ==> AUTO-APPROVE
0.20-0.78 ==> STEP-UP AUTH (OTP)
>= 0.78   ==> HARD DECLINE
      │
      ▼ (Step 7: Rust Serialization & HTTP 200)
`orjson.dumps()` response ko byte-buffer banata hai (18µs) -> Customer ko turant result milta hai!
      │
      ▼ (Step 8: Async Background Tasks - Off the Hot Path)
Background me: TreeSHAP async thread me chalta hai, Postgres me audit record likha jata hai, 
Neo4j me device graph link hota hai, aur Drift Daemon PSI check karta hai!
```

---

## 4. Module-by-Module Comprehensive Breakdown (Modules 1 to 9)

---

### MODULE 1: Python Concurrency, Asyncio & Docker Infrastructure

#### 🎯 Core Mission:
Aise high-throughput system ko traditional synchronous architecture (jaise standard Flask ya Django) par nahi chalaya ja sakta. Agar 3,100 requests per second aayengi aur har request database ya network I/O ka wait karegi, toh OS threads exhaust ho jayenge aur server collapse kar jayega. Module 1 hamare pure platform ka **concurrency aur container runtime backbone** tayyar karta hai.

#### ⚙️ Internal Mechanics & How It Works:
1. **Asynchronous Non-Blocking I/O (`asyncio`):**
   - Python ka standard `time.sleep()` pure OS thread ko block kar deta hai. Jabki `await asyncio.sleep()` OS thread ko block nahi karta, balki control wapas **Event Loop** ko de deta hai taaki wo doosri ready tasks ko execute kar sake.
   - Yeh **Cooperative Multitasking** par kaam karta hai: Coroutine khud yield karti hai jab wo network I/O ka wait kar rahi hoti hai.
2. **OS Kernel Multiplexing (`epoll` on Linux, `kqueue` on macOS):**
   - Kernel level par OS ko hazaron open TCP sockets track karne hote hain. Purana `select()` system call $O(N)$ scanning karta tha (jitne zyada sockets, utna slow).
   - Modern `epoll`/`kqueue` $O(1)$ event-driven mechanism use karta hai: Jab kisi network socket par data arrive hota hai, network card interrupt raise karta hai, kernel ready sockets ki list banata hai, aur event loop ko direct notify karta hai.
3. **CPython Internals & The GIL (Global Interpreter Lock):**
   - CPython me GIL ek mutex hai jo ek time par sirf ek OS thread ko Python bytecode execute karne deta hai.
   - Isiliye sirf multi-threading se CPU-bound tasks (jaise ML inference) fast nahi ho sakte. Solution: **Multi-Processing** (Gunicorn forks 4 isolated worker processes, har ek ka apna alag GIL aur memory space hota hai).
4. **Linux Kernel `SO_REUSEPORT`:**
   - Multiple worker processes ek hi port (e.g. 8000) par listen karte hain. Linux kernel khud incoming TCP handshake ko 4-tuple hash (`src_ip, src_port, dst_ip, dst_port`) ke through workers me bina kisi lock ke distribute kar deta hai. Isse "Thundering Herd Problem" solve ho jati hai.
5. **Docker Infrastructure & `cgroups v2`:**
   - Har service (FastAPI, Redis, Kafka, Postgres, Neo4j) isolated Docker container me chalti hai. `docker-compose.yml` me explicit resource limits (`cpus: 4.0`, `memory: 2g`) define ki gayi hain taaki koi memory leak pura host machine crash na kar de.
   - Dependent containers ke start hone se pehle `healthcheck` probes ensure karte hain ki upstream services (Kafka, Redis, Postgres) fully healthy hain, jisse `ECONNREFUSED` crashes khatam ho jate hain.

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/docker-compose.yml` (Healthchecks, cgroups limits, service dependencies)
- `my_fraud_pipeline/serving/Dockerfile` (Uvicorn worker, uvloop, Gunicorn config)

---

### MODULE 2: Distributed Message Streaming (Apache Kafka KRaft)

#### 🎯 Core Mission:
Transaction data ko payment gateway se asynchronous tarike se absorb karna bina kisi transaction loss ke. Kafka ek **durable, high-throughput buffer** ki tarah kaam karta hai jo peak traffic bursts (e.g. Diwali sale ya bot attacks) ko smoothly absorb karta hai.

#### ⚙️ Internal Mechanics & How It Works:
1. **KRaft Mode (No ZooKeeper):**
   - Kafka 7.6.0 KRaft (Kafka Raft Metadata Mode) use karta hai. ZooKeeper ka external dependency hat gaya hai. Metadata management ab internal Raft consensus quorum ke through hota hai, jisse controller failover milliseconds me ho jata hai.
2. **Append-Only Commit Log & Zero-Copy Architecture:**
   - Kafka memory me messages ko traditional queues (RabbitMQ) ki tarah temporarily hold karke delete nahi karta. Yeh disk par ek sequential append-only log file (`.log`) maintain karta hai.
   - **OS Page Cache + `sendfile()` System Call:** Jab consumer data read karta hai, Linux kernel `sendfile()` system call use karta hai. Data direct disk/Page Cache se network socket buffer me transfer hota hai bina CPU ya user-space RAM me copy huye (**Zero-Copy Architecture**).
3. **Partitioning & Murmur2 Hashing:**
   - Kafka topics (`transactions:raw`) 3 partitions me split hain.
   - Transaction producer `Murmur2(user_id)` hash algorithm use karta hai:
     $$\text{partition} = (\text{Murmur2}(\text{user\_id}) \ \& \ \text{0x7fffffff}) \pmod 3$$
   - **Crucial Rule:** Ek particular `user_id` ki saari transactions **hamesha ek hi partition** me jayengi! Isse per-user chronological ordering 100% guarantee hoti hai, jo velocity calculations ke liye mandatory hai.
4. **Delivery Semantics & Offset Commits:**
   - Feature Engine Kafka consumer `enable_auto_commit=False` par chalta hai.
   - Message process hone aur Redis/Postgres me successfully commit hone ke baad hi `reader.commit()` call hota hai (**At-Least-Once Delivery**). Agar worker crash ho jaye, toh message dobara replay hoga, kabhi drop nahi hoga.
5. **Dead Letter Queue (DLQ) & Poison Pills:**
   - Agar koi hacker ya buggy client corrupt/malformed payload bhej deta hai (e.g. invalid JSON, missing fields), toh parser fail hone par pipeline stall nahi hoti.
   - Error handler poison payload ko isolate karke special topic `transactions:dlq` me bhej deta hai aur main partition ka offset aage badha deta hai.

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/producer/main.py` (Synthetic generation, Murmur2 hashing, fraud pattern injection)
- `my_fraud_pipeline/common/kafka_reader.py` (Consumer loop, manual offset commit, error isolation)
- `my_fraud_pipeline/tests/dlq_stress_test.py` (DLQ poison payload resilience test)

---

### MODULE 3: Real-Time Feature Store (Redis 7.2 & Apache Flink)

#### 🎯 Core Mission:
Raw transaction me ML model ke liye zaroori signals nahi hote. Model ko yeh jaanna hota hai: *"Is user ne pichhle 1 ghante me kitni transactions ki? Pichhle 24 ghante ka average spend kitna tha? Kya yeh transaction user ke regular pattern se 3 standard deviations bahar hai?"* In features ko **sub-millisecond speed** par compute karna Module 3 ka kaam hai.

#### ⚙️ Internal Mechanics & How It Works:
1. **Redis 7.2 Hot Store & In-Memory Data Structures:**
   - Redis single-threaded event loop (`ae.c`) par RAM ke andar chalta hai, isiliye sub-millisecond read/write latency deta hai.
   - **Sliding Window Velocity via Sorted Sets (`ZSET`):** Har user ke liye ek ZSET key hoti hai (`user:txns:{user_id}`).
     * **Score:** Transaction ka Unix Epoch timestamp in seconds (`now_ts`).
     * **Member:** Transaction details (`ts:amount:merchant_id`).
   - Under the hood, Redis ZSET do data structures ka hybrid hai:
     1. **Hash Table:** $O(1)$ me check karta hai ki member already exist karta hai ya nahi.
     2. **Skip List:** Multi-level linked list jo $O(\log N)$ me range queries aur score sorting handle karti hai bina B-Tree ke complex rebalancing overhead ke.
2. **Lazy Pruning & Velocity Feature Derivation:**
   - Har naye event par, Feature Engine skip list se purana data prune karta hai:
     ```redis
     ZREMRANGEBYSCORE user:txns:{user_id} -inf (now_ts - 86400)
     ```
     Yeh command 24 ghante se purani transactions ko turant delete kar deti hai ($O(\log N + M)$).
   - Fir `ZRANGEBYSCORE` se 1 ghante (`now_ts - 3600`) aur 24 ghante ke members fetch kiye jaate hain.
   - In-memory computation se 16 features bante hain: `txn_velocity_1h`, `txn_velocity_24h`, `amount_mean_24h`, `amount_std_24h`, `amount_zscore`, `distinct_merchants_1h`, `impossible_travel_flag`, etc.
3. **Redis Pipelining (TCP Round Trip Time Elimination):**
   - Agar hum 4 commands alag-alag bhejenge, toh 4 network round trips lagenge (~2ms).
   - Redis Pipelining (`pipeline.execute()`) se saari commands ek single TCP socket buffer me pack ho kar jati hain aur ek hi response me saare results aate hain. Latency 1.5ms se drop hokar **0.5ms** ho jati hai!
4. **Enterprise Scale-Up Blueprint: Apache Flink (Stateful Stream Processing):**
   - Jab throughput 100k+ TPS pahuchti hai, tab Redis bottleneck ban sakta hai. Module 3 me humne Apache Flink ka architectural blueprint tayyar kiya hai:
     * **Event-Time Processing & Watermarking:** Network delays ke bawajood out-of-order transactions ko handle karta hai using `BoundedOutOfOrderness`.
     * **RocksDB State Backend:** Flink user state ko JVM heap se bahar local NVMe SSD par embedded LSM-Trees me rakhta hai, jisse Java Garbage Collection (GC) pauses zero ho jaate hain.
     * **Chandy-Lamport Checkpointing:** Asynchronous distributed snapshots draw karta hai bina streaming flow ko roke (Exactly-Once state semantics).

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/feature_engine/main.py` (Consumer loop, Redis ZSET pruning, feature math)
- `my_fraud_pipeline/feature_engine/redis_store.py` (Redis connection pooling & pipelines)
- `my_fraud_pipeline/flink_streaming/fraud_flink_job.py` (PyFlink stateful window blueprint)

---

### MODULE 4: Multi-Tier Storage & Graph Analytics (Neo4j & Postgres)

#### 🎯 Core Mission:
Har tarah ke data ka apna optimal storage engine hota hai. Real-time inference ke liye ultra-fast cache chahiye, financial ledger ke liye strict ACID guarantees chahiye, aur organized crime syndicates / fraud rings ko pakadne ke liye graph relationship engine chahiye.

#### ⚙️ Internal Mechanics & How It Works:
1. **Multi-Tier Cache Hierarchy (L1 vs L2):**
   - **L1 In-Memory `cachetools.TTLCache` (Per-Worker Process Heap):**
     * Latency: **< 5 microseconds ($<0.005\text{ms}$)**.
     * Fast-burst card testing bot attacks me same card par microsecond intervals me hits aate hain. L1 cache network trip ko bypass karke process memory se hi features return kar deti hai.
   - **L2 Redis 7.2 Feature Store:**
     * Latency: **~0.5ms to 1.2ms**.
     * Network boundary par shared cache jo horizontally scaled saare FastAPI workers ke beech feature state sync rakhti hai.
2. **Neo4j 5.20 Graph Database (Fraud Rings & Mule Accounts):**
   - Traditional relational databases me fraud rings dhundhne ke liye multi-table SQL `JOIN`s likhne padte hain, jo $O(N^k)$ exponential cost lete hain aur production DB ko freeze kar dete hain.
   - Neo4j **Index-Free Adjacency (IFA)** use karta hai: Har graph node ke paas apne adjacent edges ka direct memory pointer hota hai. Graph traversal ka time graph ke total size par depend nahi karta, sirf traversed path par depend karta hai ($O(k)$).
   - **Cypher Query Execution:**
     ```cypher
     MATCH (u:User {user_id: $user_id})-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(other:User)
     OPTIONAL MATCH (other)-[:FLAGGED_FRAUD]->(f:FraudEvent)
     RETURN count(DISTINCT other) AS shared_device_count,
            min(length(shortestPath((u)-[*]-(f)))) AS hop_distance_to_fraud
     ```
   - Agar ek device 5 se zyada accounts share kar raha hai ya confirmed fraudster se hop distance $\le 2$ hai, toh feature store me `shared_device_count` aur `hop_distance_to_fraud` update ho jaate hain!
3. **PostgreSQL 16 System of Record (`asyncpg` Binary Protocol):**
   - System of Record jahan har scored transaction, prediction probability, aur model audit log permanently store hota hai.
   - `asyncpg` Python ka fastest PostgreSQL driver hai jo standard text-based SQL protocol ke bajaye PostgreSQL ka native **binary wire protocol** use karta hai, jisse client-side type-parsing overhead khatam ho jata hai.

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/graph_features/writer.py` & `queries.py` (Neo4j driver, Cypher graph queries)
- `my_fraud_pipeline/serving/db_async.py` (asyncpg connection pool, binary streaming inserts)
- `my_fraud_pipeline/postgres_init/001_init_schema.sql` (Relational schemas: predictions, snapshots, labels)

---

### MODULE 5: Ultra-Low Latency Serving & Inference (FastAPI & ONNX Runtime)

#### 🎯 Core Mission:
Sustained 3,100+ requests per second load ke under sub-7ms P99 inference deliver karna. Pure serving stack ko microsecond-level par tune kiya gaya hai.

#### ⚙️ Internal Mechanics & How It Works:
1. **Production ASGI Stack (FastAPI + Gunicorn + UvicornWorker + `uvloop`):**
   - Standard Python asyncio loop ke bajaye `uvloop` use hota hai (jo Node.js ke underlying C-library `libuv` par built hai). Yeh Python event loop ko C-speed par execute karta hai.
   - Gunicorn master process 4 worker processes spawn karta hai, jisse har worker ko apna core aur independent event loop milta hai.
2. **High-Speed Serialization via `orjson` (Rust Core with SIMD):**
   - Standard Python `json.dumps()` pure CPython recursion use karta hai jo CPU cycles waste karta hai.
   - `orjson` Rust me likha gaya hai aur modern CPU ke **AVX2 SIMD registers** use karta hai. JSON payload serialization ka time standard 1.2ms se gir kar sirf **18 microseconds ($0.018\text{ms}$)** reh jata hai!
3. **ONNX Runtime (C++ Engine) & Python GIL Release:**
   - Raw XGBoost / Scikit-Learn Python inference CPython interpreter ke andar run hota hai, jahan GIL doosre threads ko block kar deta hai.
   - Model ko **ONNX (Open Neural Network Exchange)** format me export kiya gaya hai.
   - `onnxruntime` C++ inference engine call hone par `Py_BEGIN_ALLOW_THREADS` macro execute hota hai: Python ka GIL completely release ho jata hai! C++ engine CPU ke AVX-512 SIMD vector instructions par direct tensor matrix multiplication karta hai.
   - Serving container me `intra_op_num_threads=1` configure kiya gaya hai taaki CPU cores par thread-switching aur context-switch overhead na ho. Single inference sirf **0.8ms** leti hai!
4. **Dynamic Micro-Batching (`ModelBatcher`):**
   - Jab high-concurrency traffic aata hai, toh har ek single transaction ke liye alag se inference run karne ke bajaye, `ModelBatcher` ek async queue (`asyncio.Queue`) me incoming requests ko hold karta hai.
   - **Coalescing Logic:** Queue max 50 items collect karti hai ya maximum 5ms wait karti hai (whichever happens first).
   - Jaise hi batch bharta hai ya timer expire hota hai, ek single 50-row tensor ONNX engine ko diya jata hai. 50 transactions sirf **1.5ms** me score ho jati hain (effective cost per transaction: **0.03ms**)! Coroutines Python `Future` par suspended rehti hain aur batch complete hote hi instant wake-up ho jati hain.

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/serving/main.py` (FastAPI app, lifespan management, `/score` endpoint)
- `my_fraud_pipeline/serving/scoring_onnx.py` (ONNX Runtime session, SIMD inference, ModelBatcher)
- `my_fraud_pipeline/training/export_onnx.py` (XGBoost to ONNX conversion pipeline)

---

### MODULE 6: Fraud Machine Learning Mathematics & Decision Theory

#### 🎯 Core Mission:
Extreme class imbalance (99.8% normal vs 0.2% fraud) ke environment me aisa robust model train karna jo maximum fraud pakad sake bina legitimate customers ko pareshan kiye, aur har decision ke piche ka mathematical reason explain kar sake.

#### ⚙️ Internal Mechanics & How It Works:
1. **XGBoost 2.1 Mathematics & 2nd-Order Taylor Objective:**
   - XGBoost gradient boosted decision trees train karta hai. Traditional gradient descent ke mukabale, XGBoost loss function ka **2nd-order Taylor expansion** calculate karta hai:
     $$\mathcal{L}^{(t)} \approx \sum_{i=1}^n \left[ l(y_i, \hat{y}^{(t-1)}) + g_i f_t(x_i) + \frac{1}{2} h_i f_t^2(x_i) \right] + \Omega(f_t)$$
     Jahan $g_i$ first derivative (gradient) hai aur $h_i$ second derivative (Hessian) hai. Yeh optimal split point aur leaf weights analytically compute kar leta hai.
2. **Handling Extreme Class Imbalance (`scale_pos_weight`):**
   - Agar dataset me 1,000,000 transactions hain aur sirf 2,000 fraud hain (0.2%), toh standard loss function fraud examples ko ignore kar dega (99.8% accuracy bina kuch kare mil jayegi!).
   - Hamara pipeline mathematical weight inject karta hai:
     $$\text{scale\_pos\_weight} = \frac{N_{\text{negative}}}{N_{\text{positive}}} = \frac{998,000}{2,000} \approx 499.0$$
   - Isse fraud sample miss hone par model ko 499 guna zyada penalty lagti hai.
3. **Evaluation Metrics: PR-AUC vs. The ROC-AUC Fallacy:**
   - Fraud detection me standard **ROC-AUC ek bada trap hai**. Kyunki True Negatives ($N = 998,000$) itne huge hote hain ki False Positive Rate ($\frac{FP}{FP + TN}$) hamesha artificially zero ke kareeb rehta hai, jisse ROC-AUC 0.99 dikhayi deta hai chahe model kitna bhi kharab ho!
   - Isiliye hum **Precision-Recall AUC (PR-AUC)** use karte hain, jisme denominator me sirf True Positives aur False Positives hote hain. PR-AUC fraud detection ka true benchmark hai.
4. **Cost-Sensitive $F_{0.5}$ Score Optimization:**
   - Default threshold $\tau = 0.5$ fraud ke liye bekaar hota hai.
   - Bank ke business model me False Positive (genuine user friction) ki cost False Negative (fraud loss) se alag hoti hai. Hum $F_\beta$ score optimize karte hain with $\beta = 0.5$:
     $$F_{0.5} = (1 + 0.5^2) \frac{\text{Precision} \cdot \text{Recall}}{(0.5^2 \cdot \text{Precision}) + \text{Recall}} = 1.25 \frac{\text{Precision} \cdot \text{Recall}}{0.25 \cdot \text{Precision} + \text{Recall}}$$
   - Yeh precision ko recall ke mukabale double weight deta hai. Validation set par grid scan karke optimal threshold nikalta hai: $\tau^* \approx 0.78$.
5. **Three-Tier Decision Routing Matrix:**
   - Model ke probability score $\hat{p}$ ko 3 zones me baanta gaya hai:
     * **Tier 1: AUTO-APPROVE ($\hat{p} < 0.20$):** 98.5% genuine traffic bina kisi friction ke instant approve hota hai.
     * **Tier 2: STEP-UP AUTH ($0.20 \le \hat{p} < 0.78$):** Borderline suspicious cases. Transaction seedhe decline nahi hoti, customer ko SMS OTP ya 3D-Secure challenge bheja jata hai.
     * **Tier 3: HARD DECLINE ($\hat{p} \ge 0.78$):** High-confidence fraud. Transaction turant decline hoti hai aur card temporarily lock ho jata hai.
6. **TreeSHAP Explainability (Async Non-Blocking):**
   - Har declined/step-up transaction ke liye legal compliance (RBI/GDPR) demand karti hai ki bank customer ko reason bataye.
   - TreeSHAP algorithm game theory ke Shapley values use karke har feature ka exact contribution nikalta hai.
   - **Crucial Engineering:** TreeSHAP computation $O(T \cdot L \cdot D^2)$ time leti hai (~25ms). Isko main event loop me chalane par serving latency choke ho jayegi! Isiliye isse `asyncio.to_thread()` ke through background thread pool me offload kiya jata hai. Client ko response mil chuka hota hai, aur SHAP values baad me PostgreSQL me update ho jati hain!

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/training/train.py` (scale_pos_weight math, PR-AUC, F0.5 grid scan)
- `my_fraud_pipeline/serving/scoring.py` (TreeSHAP calculation, decision threshold matrix)

---

### MODULE 7: Dynamic Model Routing & Bayesian Bandits

#### 🎯 Core Mission:
Production me naya ML model deploy karna risky hota hai. Traditional static 50/50 A/B testing sub-optimal model par 50% traffic bhej kar lakho rupaye ka fraud loss karwa sakti hai. Module 7 **Bayesian Multi-Armed Bandits (MAB)** implement karta hai jo mathematically provable minimal regret ke sath traffic allocate karta hai.

#### ⚙️ Internal Mechanics & How It Works:
1. **Multi-Armed Bandit vs Static A/B Testing:**
   - Static A/B testing me agar Challenger model kharab perform kar raha hai, tab bhi experiment khatam hone tak (e.g. 2 hafte) 50% users ko wahi kharab model score karega (**Linear Regret: $O(T)$**).
   - Multi-Armed Bandit reinforcement learning style me continuously learn karta hai. Jaise hi usse dikhta hai ki ek model behtar hai, wo traffic automatically uski taraf shift kar deta hai (**Logarithmic Regret: $O(\ln T)$**, Lai-Robbins Lower Bound).
2. **Bayesian Thompson Sampling Mechanics:**
   - Har model arm $k$ ke fraud detection reward ko hum **Bernoulli likelihood** maante hain.
   - Iska mathematical conjugate prior **Beta Distribution** $\text{Beta}(\alpha_k, \beta_k)$ hota hai:
     * $\alpha_k$: Number of successful detections + 1 (prior).
     * $\beta_k$: Number of missed frauds or false alarms + 1 (prior).
   - Scoring ke waqt, router har model ke distribution se ek random parameter draw karta hai:
     $$\theta_k \sim \text{Beta}(\alpha_k, \beta_k)$$
     Aur jis model ka $\theta_k$ highest hota hai, wo us transaction ko score karta hai:
     $$k^* = \arg\max_k \theta_k$$
   - **Exploration vs Exploitation:** Jab tak observations kam hain, distribution broad hoti hai (high exploration). Jaise hi observations badhti hain, variance shrink ho jata hai aur traffic best model par exploit hone lagta hai.
3. **Deterministic MD5 Hashing (Horizontal Pod Consistency):**
   - Agar transaction retry hoti hai, toh deterministic outcome hona chahiye.
   - Router transaction ID ka MD5 hash nikalta hai:
     $$\text{seed} = \text{int}(\text{MD5}(\text{txn\_id}), 16) \pmod{2^{32} - 1}$$
   - Yeh seed NumPy ke `PCG-64` random number generator ko feed kiya jata hai. Iska faayda: Horizontally scaled 10 alag-alag Kubernetes pods me bhi same transaction ID hamesha ek hi model arm par route hogi!
4. **Shadow Deployment Mode:**
   - Production me naya model live karne se pehle use `ScoringMode.SHADOW` me run kiya jata hai.
   - Champion model ka score customer-facing decision banta hai. Challenger model asynchronously parallel me wahi vector score karta hai aur apna score DB me log karta hai. Jab shadow model statistical significance prove kar deta hai, tabhi use bandit pool me entry milti hai.

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/serving/scoring.py` (`pick_model()` function, Thompson sampling, MD5 seed)
- `my_fraud_pipeline/serving/ab_test_metrics.py` (Posterior Beta update, cumulative regret tracking)

---

### MODULE 8: Continuous MLOps, Drift Detection & Feedback Loops

#### 🎯 Core Mission:
Production me machine learning models static nahi hote; unka performance samay ke sath degrade hota hai (**Concept Drift & Data Drift**). Fraudsters naye tareeqe dhoondhte hain. Module 8 ek **fully automated self-healing MLOps loop** provide karta hai jo data drift detect karta hai, model retrain karta hai, aur bina single connection drop kiye model ko memory me hot-reload karta hai.

#### ⚙️ Internal Mechanics & How It Works:
1. **The MLOps Lifecycle Loop:**
   ```
   [Live Traffic] ──> [Feature Snapshots (Postgres)] ──> [Ground-Truth Labels (14-60 Days)]
          ▲                                                                 │
          │                                                                 ▼
   [Zero-Downtime Hot Reload] ◄── [MLflow Registry] ◄── [Automated Retrain (XGBoost)]
   ```
2. **Population Stability Index (PSI) — Feature Drift Detection:**
   - Har 5 minute me background daemon pichhle 6 ghante ke live features ko 72-ghante ke baseline distribution se compare karta hai.
   - Continuous features ko 10 quantiles me baant kar PSI calculate kiya jata hai:
     $$\text{PSI} = \sum_{b=1}^{10} \left( P_b - B_b \right) \times \ln\left( \frac{P_b}{B_b} \right)$$
     (Jahan $P_b$ live probability hai aur $B_b$ baseline probability hai).
   - Zero division se bachne ke liye **Laplace probability clipping** ($\epsilon = 10^{-4}$) use hoti hai.
   - **Decision Rule:**
     * $\text{PSI} < 0.10$: Stable (No action).
     * $0.10 \le \text{PSI} < 0.25$: Slight Drift (Warning).
     * $\text{PSI} \ge 0.25$: **Significant Drift** $\rightarrow$ Automatic Retraining Pipeline Trigger!
3. **Kolmogorov-Smirnov (KS) Test — Prediction Probability Drift:**
   - Two-Sample KS-Test check karta hai ki kya model ke output prediction distribution ka shape badal gaya hai:
     $$D = \sup_x |F_{\text{live}}(x) - F_{\text{baseline}}(x)|$$
   - Agar $p$-value $< 0.01$ aati hai, toh statistically significant prediction drift detect hota hai.
4. **CRITICAL: The Retraining Safety Rule (Attack Drift vs Concept Drift):**
   - **Industry-Standard Golden Rule:** Agar prediction scores me drift aaya hai ($p < 0.01$), lekin features me drift nahi hai ($\text{PSI} < 0.25$), toh **KABHI BHI MODEL KO RETRAIN NAHI KARNA CHAHIYE!**
   - Kyu? Kyunki iska matlab hai ki platform par ek active coordinated botnet attack chal raha hai! Unlabelled attack traffic par model retrain karne se model "poison" ho jayega aur fraud ko normal samajhne lagega.
   - Is case me system **Security Alert (SOC)** raise karta hai, retraining nahi! Retraining sirf tab trigger hoti hai jab features drift hote hain ($\text{PSI} \ge 0.25$) aur relational cooldown lock (60 minutes) expire ho chuka ho.
5. **Zero-Downtime Hot-Reloading (`ModelHandle` Atomic Swap):**
   - Naya model train hokar MLflow Model Registry me register hota hai.
   - FastAPI server ko restart karne ki zaroorat nahi padti! Serving layer ek `ModelHandle` wrapper use karta hai. Python me reference assignment:
     ```python
     self._current_session = new_ort_session
     ```
     Linux x86-64 architecture par yeh **64-bit atomic pointer swap** hota hai jo **< 1 microsecond** leta hai. In-flight requests purane model pointer par finish hoti hain aur nayi requests instant naye model par switch ho jati hain — zero dropped packets, zero restart latency!
6. **Delayed Chargeback Maturation & Survival Bias:**
   - Fraud labels instant nahi aate; actual customer bank me complaint 14 se 60 din baad karta hai (**Label Delay**).
   - Training pipeline me recent unlabelled data par train karne se False Negative pollution ho sakti hai. Isiliye training data hamesha strictly observation window ke bahar matured labels par `INNER JOIN` karke banaya jata hai.

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/monitoring/drift.py` (PSI math, KS-test, cooldown lock, subprocess trigger)
- `my_fraud_pipeline/serving/model_registry.py` (MLflow pull, atomic pointer swap hot-reloading)
- `my_fraud_pipeline/feedback/simulate_labels.py` (Simulated chargeback delay, maturation curves)

---

### MODULE 9: Production Resiliency & Performance Benchmarking

#### 🎯 Core Mission:
Production me cheezein fail hongi hi: Redis down ho sakta hai, network switch me packet drop ho sakta hai, traffic achanak 10x spike ho sakta hai. Module 9 ensure karta hai ki system **gracefully degrade** ho bina crash kiye, aur realistic load testing ke through tail latencies ko verify karta hai.

#### ⚙️ Internal Mechanics & How It Works:
1. **Circuit Breakers (`pybreaker`) & Graceful Degradation:**
   - Agar Redis feature store crash ho jaye ya network timeout hone lage, toh har incoming request 2-second timeout ka wait karegi, jisse FastAPI ke saare connection pools exhaust ho jayenge aur pura serving gateway crash ho jayega (**Cascading Failure**).
   - Solution: **Circuit Breaker State Machine:**
     ```
     [CLOSED: Normal Flow] ──(3 consecutive failures)──> [OPEN: Fail-Fast (Neutral Fallback)]
              ▲                                                       │
              │                                                (10s timeout)
              │                                                       ▼
     [Reset Counter] ◄──(1 successful request)─── [HALF-OPEN: Probe Canary]
     ```
   - **Fail-Fast Neutral Imputation:** Jab circuit `OPEN` hota hai, toh serving layer Redis ko call hi nahi karta (<0.002ms me fail-fast hota hai). Wo turant ek neutral zero-vector ($17 \times 0.0$) feature supply kar deta hai. Model safely fallback mode me score karta hai aur service zinda rehti hai!
2. **Distributed Load Testing with Locust & `gevent` Greenlets:**
   - `locust` benchmark tool use karke distributed load generate kiya gaya.
   - Traditional OS threads heavy hote hain (har thread 8MB stack space leta hai). Locust Python ke lightweight user-space micro-threads (**Greenlets via `gevent`**) use karta hai. Ek single test machine 200+ concurrent virtual users ka stochastic traffic generate karti hai.
3. **The Coordinated Omission Problem (Gil Tene's Latency Fallacy):**
   - Standard load testing tools (jaise Apache Bench ya poorly configured JMeter) **Closed-Loop Scheduling** use karte hain: wo ek request ka response aane ke baad hi doosri request bhejte hain.
   - Agar server 2 second ke liye freeze ho jaye, toh closed-loop tool request bhejna hi band kar deta hai! Isse measured P99 latency artificially low dikhti hai. Is deceptive bias ko **Coordinated Omission** kehte hain.
   - Hamara load testing harness **Open-Loop Stochastic Dispatch** simulate karta hai: Requests server ke response ka wait kiye bina scheduled timestamps par burst hoti hain, jo real-world production traffic ka exact reflection hai.
4. **Tail Latency Profiling (P50, P95, P99, P99.9):**
   - High-throughput payment systems me Average latency misleading hoti hai. Agar average 3.9ms hai lekin P99 50ms hai, toh har 100 me se 1 customer ka checkout slow hoga.
   - Hamara pipeline sub-7ms P99 SLA rigorously enforce karta hai:
     * **P50 (Median):** 2.1ms
     * **P95:** 3.1ms
     * **P99 (SLA Bound):** **3.35ms** (Well below 7.0ms target!)
     * **P99.9 (Extreme Tail):** 5.8ms
5. **Streamlit Real-Time Observability Dashboard:**
   - Port 8501 par live web dashboard run hota hai jo continuous operational metrics display karta hai: Live throughput, P99 latency trends, Model approval vs decline distribution, Confusion matrix, aur Drift history.
   - **Query Stampede Protection:** `@st.cache_data(ttl=5)` query caching use hoti hai taaki multiple browser refreshes se PostgreSQL par stampede load na pade.
   - **SQL Zero-Division Protection:** SQL aggregation queries me `NULLIF(total, 0)` use hota hai taaki empty database cold-start me dashboard crash na ho.

#### 📁 Codebase Mapping:
- `my_fraud_pipeline/serving/circuit_breaker.py` (Pybreaker FSM, fallback feature vector)
- `my_fraud_pipeline/tests/load/locustfile.py` (Locust greenlet load generator)
- `my_fraud_pipeline/tests/load_test_serving.py` (Local concurrent performance harness)
- `my_fraud_pipeline/monitoring/dashboard.py` (Streamlit visualization dashboard)

---

## 5. 5 Transaction Archetypes Ka Real-World Safar (Scenarios)

Har transaction identical nahi hoti. Hamara pipeline **5 primary behavioral archetypes** ko handle karta hai. Chalo dekhte hain har ek transaction type ke sath system ke andar kya hota hai:

---

### 🟢 SCENARIO 1: Normal Retail Cardholder (Rahul)
- **Profile:** Rahul Mumbai me rehta hai. Wo sham ko ek local grocery store par 180 INR ka payment karta hai.
- **System Behavior:**
  1. **Ingress:** Transaction POST `/score` par aati hai. Gunicorn worker parse karta hai.
  2. **Feature Store:** L1 cache miss hoti hai, L2 Redis lookup hota hai.
     - `txn_velocity_1h = 1` (Pichhle 1 ghante me pehli transaction).
     - `amount_zscore = -0.15` (180 INR Rahul ke normal spend ke bilkul kareeb hai).
     - `impossible_travel_flag = 0` (Home country IN match ho gayi).
  3. **ML Inference:** ONNX model vector evaluate karta hai: $\hat{p} = 0.012$ (1.2% fraud probability).
  4. **Decision:** Threshold check: $0.012 < 0.20 \rightarrow$ **TIER 1: AUTO-APPROVE**.
  5. **Outcome:** Rahul ka card instant approve hota hai without any OTP! Total time: **2.8ms**.

---

### 🔴 SCENARIO 2: Velocity Spike / Botnet Card-Testing Attack
- **Profile:** Hacker ne dark web se chori kiye hue card numbers khareede hain. Usne ek bot script chalayi jo rapid 20-30 micro-transactions (10 INR, 25 INR) bhej rahi hai check karne ke liye ki card active hai ya nahi.
- **System Behavior:**
  1. **Ingress:** Ek hi `user_id` par 500ms ke andar 10 requests aati hain. Murmur2 hashing sabko same Kafka partition par daalta hai.
  2. **Feature Store:** Pehle 2 transaction normal nikalte hain. Lekin 3rd transaction par Redis `ZSET` skip-list me count rapidly badhta hai:
     - `txn_velocity_1h = 11` (Limit breach!).
     - `distinct_merchants_1h = 6`.
  3. **ML Inference:** Feature vector me extreme velocity signals aate hain. Model score shoot up hota hai: $\hat{p} = 0.89$.
  4. **Decision:** Threshold check: $0.89 \ge 0.78 \rightarrow$ **TIER 3: HARD DECLINE**.
  5. **Outcome:** Transaction turant block hoti hai. Background me TreeSHAP run hota hai, jo batata hai ki top contributing feature `txn_velocity_1h` tha. Cardholder ko alert jata hai aur card temporarily freeze ho jata hai!

---

### 🟡 SCENARIO 3: Sudden Amount Outlier
- **Profile:** Priya jiska normal monthly spend 500–2,000 INR hota hai, achanak ek luxury electronics store se 95,000 INR ka transaction karti hai.
- **System Behavior:**
  1. **Feature Store:** User baseline hash `HGETALL` calculate hota hai:
     - `amount_mean_24h = 1,200`
     - `amount_std_24h = 450`
     - Current amount = 95,000 INR $\rightarrow$ `amount_zscore = (95000 - 1200) / 450 = +208.4\sigma$ (Extreme Outlier!).
  2. **ML Inference:** Model output: $\hat{p} = 0.54$. Model fraud ke baare me 100% sure nahi hai kyunki IP aur device Priya ka hi hai, lekin amount suspicious hai.
  3. **Decision:** Threshold check: $0.20 \le 0.54 < 0.78 \rightarrow$ **TIER 2: STEP-UP AUTHENTICATION**.
  4. **Outcome:** System transaction ko seedhe decline karke customer ko naraz nahi karta! Wo screen par 3D-Secure 2-Factor OTP challenge popup bhejta hai. Priya OTP enter karti hai aur payment successfully complete ho jata hai.

---

### 🟣 SCENARIO 4: Impossible Travel Attack
- **Profile:** Amit ne 2 second pehle Delhi (India) ke ek retail store par card use kiya. 5 second baad usi card se London (UK) ke ek merchant par transaction trigger hoti hai (Card Cloning / Compromised Token).
- **System Behavior:**
  1. **Feature Store:** Redis HASH `last_seen` check karta hai:
     - `last_country = 'IN'`, `current_country = 'GB'`
     - $\Delta t = 5\text{ seconds}$.
     - Delhi se London ki physical distance ~6,700 km hai. 5 second me yeh travel physical physics ke laws ke khilaf hai ($>4,800,000\text{ km/h}$ required!).
     - Feature Engine sets `impossible_travel_flag = 1`.
  2. **ML Inference:** Model me `impossible_travel_flag` sabse dominating weight carry karta hai. ONNX score: $\hat{p} = 0.96$.
  3. **Decision:** $0.96 \ge 0.78 \rightarrow$ **TIER 3: HARD DECLINE**.
  4. **Outcome:** Payment instantly decline hoti hai. Bank ke SOC fraud dashboard par instant high-priority red alert generate hota hai.

---

### 🔵 SCENARIO 5: Graph Mule Syndicate Ring
- **Profile:** Ek organized cybercrime gang 8 alag-alag fake accounts (money mules) use kar rahi hai, lekin sab transactions ek hi rooted Android phone se ki ja rahi hain.
- **System Behavior:**
  1. **Ingress:** Transaction normal amount ki hai, isiliye individual velocity safe lagti hai.
  2. **Neo4j Graph Enrichment:** Stream consumer Neo4j graph me transaction nodes link karta hai:
     - `(User)-[:USED_DEVICE]->(Device)`
     - Cypher query match karti hai ki is `device_id` se already 7 accounts registered hain, jinme se 2 accounts pichhle hafte fraud mark ho chuke hain.
     - `shared_device_count = 7`, `hop_distance_to_fraud = 1`.
  3. **Serving Layer:** Redis feature store me graph risk signals push ho chuki hain.
  4. **ML Inference:** Model isolated user features ke bajaye relational features detect karta hai: $\hat{p} = 0.84$.
  5. **Decision:** $0.84 \ge 0.78 \rightarrow$ **TIER 3: HARD DECLINE**.
  6. **Outcome:** Pure syndicate ka transaction network block ho jata hai!

---

## 6. Latency Budget Breakdown (Har Ek Millisecond Ka Hisaab)

Hamara target P99 Latency **< 7.0 milliseconds** hai. Production benchmark testing me actual latency **~3.35ms** aayi hai. Neeche table me dekho har ek stage kitna time leta hai:

| Pipeline Stage | Technology | Time Spent (ms) | Cum. Time (ms) | Kyu Itna Fast Hai? |
|:---|:---|:---:|:---:|:---|
| **1. TCP Handshake & Socket Dispatch** | Linux Kernel `SO_REUSEPORT` | 0.05 ms | 0.05 ms | Kernel level 4-tuple zero-lock dispatching |
| **2. Ingress Deserialization & Validation** | Pydantic v2 (Rust Core) | 0.25 ms | 0.30 ms | SIMD-accelerated Rust parsing, no Python loop |
| **3. L1 Cache Check** | `cachetools.TTLCache` | 0.005 ms | 0.305 ms | Direct CPython process heap dictionary lookup |
| **4. L2 Feature Store Lookup** | Redis 7.2 (Async Pipeline) | 0.65 ms | 0.955 ms | In-memory RAM lookup, single TCP pipelined RTT |
| **5. Dynamic Micro-Batching Wait** | `ModelBatcher` Queue | 1.10 ms | 2.055 ms | Requests coalesced (CPU yielded to event loop) |
| **6. C++ Model Inference** | ONNX Runtime (AVX-512) | 0.85 ms | 2.905 ms | GIL released, C++ parallel SIMD matrix math |
| **7. Bandit Routing & Threshold Logic** | Thompson Sampling + F0.5 | 0.04 ms | 2.945 ms | Deterministic MD5 hash + vectorized decision |
| **8. Response Serialization** | `orjson` (Rust SIMD) | 0.018 ms | 2.963 ms | AVX2 SIMD byte buffer construction |
| **9. Network Egress Buffer Write** | Linux TCP Buffer | 0.387 ms | **3.35 ms** | Non-blocking socket write |
| **--- ASYNC (OFF THE CRITICAL PATH) ---** | --- | --- | --- | **Client response ke BAAD background me hota hai** |
| **10. Explainability (TreeSHAP)** | C++ TreeSHAP (`asyncio.to_thread`) | ~25.0 ms | Async | Background thread pool, event loop free |
| **11. Audit Logging** | PostgreSQL 16 (`asyncpg`) | ~1.8 ms | Async | Binary streaming batch insert |
| **12. Graph Entity Sync** | Neo4j 5.20 Driver | ~4.5 ms | Async | Periodic streaming ingestion |

---

## 7. Glossary of Complex Technical Terms (Hinglish Demystified)

Yahan saare heavy engineering aur ML terms ko simple Hindi-English me real-world analogies ke sath explain kiya gaya hai:

### 1. Non-Blocking I/O & Event Loop
- **Technical Definition:** Ek single OS thread jo network request bhejne ke baad wait nahi karta, balki doosre kaam karne lagta hai.
- **Analogy (Restaurant Waiter):** Ek waiter ne Table 1 ka order kitchen me diya. Synchronous waiter kitchen ke bahar khada rahega jab tak khana na ban jaye (doosri tables bhookhi rahengi). Asynchronous waiter kitchen me order dekar turant Table 2 par order lene chala jata hai. Jaise hi Table 1 ka khana banta hai (I/O event), wo jaakar serve kar deta hai.

### 2. `epoll` / `kqueue` (I/O Multiplexing)
- **Technical Definition:** Linux (`epoll`) aur BSD/macOS (`kqueue`) ke kernel system calls jo hazaron network connections ko $O(1)$ efficiency se monitor karte hain.
- **Analogy (Doorbell System):** Agar building me 1,000 flats hain. Pehle guard har flat ka darwaza khatkhata kar poochhta tha "Koi packet aaya hai kya?" ($O(N)$ Select/Poll). `epoll` me har flat par ek electronic doorbell laga di gayi hai. Jab packet aata hai, sirf wahi doorbell bajti hai, aur guard ko pata hota hai kahan jana hai ($O(1)$).

### 3. Cooperative vs Preemptive Multitasking
- **Technical Definition:** Preemptive multitasking me OS zabardasti thread ko rok kar doosre thread ko CPU deta hai. Cooperative multitasking (`asyncio`) me task khud bolta hai (`await`) ki "Mera network wait chal raha hai, tum CPU doosre ko de do."
- **Analogy:** Preemptive matlab traffic police jo laathi maar kar gadi rok deti hai. Cooperative matlab samajhdar drivers jo zebra crossing dekh kar khud gaadi rok lete hain.

### 4. `SO_REUSEPORT` & Thundering Herd Problem
- **Technical Definition:** Linux kernel feature jo multiple independent processes ko ek hi network port par bind karne deta hai.
- **Analogy:** Ek bank me 4 cashier counters hain aur bahar 1 hi main entrance door hai. Purane system me jab darwaza khulta tha, charo cashier ek sath chillate the "Main loonga customer!" (Thundering Herd lock contention). `SO_REUSEPORT` me darwaze par ek security guard khada hai jo har customer ko systematically Counter 1, 2, 3, 4 par bhejta hai bina kisi aapas ki ladaai ke.

### 5. Append-Only Commit Log & Zero-Copy Architecture
- **Technical Definition:** Kafka ka storage model jahan records file ke end me sequentially add hote hain, aur Linux `sendfile()` system call data ko disk Page Cache se direct network card me transfer karta hai.
- **Analogy:** Ek traditional munshi register me purani entries ko kaat-peet nahi karta, hamesha nayi line me likhta hai (fast writing). Zero-copy matlab parcel godown se direct delivery van me chadha diya gaya, munshi ne apne desk par laakar khol kar dobara pack nahi kiya.

### 6. Kafka Consumer Group Rebalance Storm
- **Technical Definition:** Jab ek consumer crash hota hai ya heartbeat miss karta hai, toh Kafka cluster saare consumers ka kaam temporarily pause kar deta hai partitions redistribute karne ke liye.
- **Analogy:** Ek factory assembly line me 3 mazdoor kaam kar rahe hain. Ek mazdoor paani peene gaya, toh supervisor ne alarm baja kar baki dono mazdooron ka kaam bhi 5 minute ke liye rok diya taaki nayi duty chart ban sake. Is pause ko "Rebalance Storm" kehte hain.

### 7. Dead Letter Queue (DLQ) & Poison Pills
- **Technical Definition:** Corrupted ya unparseable messages ko alag error topic me bhej dena taaki main consumer pipeline crash ya stall na ho.
- **Analogy:** Post office me agar kisi envelope par address phata hua hai ya samajh nahi aa raha, toh postman baaki 10,000 chitthiyan baantna band nahi karta. Wo us phate envelope ko ek alag "Doubtful Box" (DLQ) me daal deta hai aur aage badh jata hai.

### 8. Redis SkipList (Sorted Sets `ZSET`) vs B-Tree
- **Technical Definition:** Redis Sorted Set values ko sort rakhne ke liye B-Tree ke bajaye SkipList use karta hai — ek multi-layered linked list jisme probabilistic forward pointers hote hain.
- **Analogy (Express vs Local Train):** Local train har station par rukti hai ($O(N)$). Express train sirf bade junction par rukti hai. SkipList me multiple tracks hote hain: top track par 4 station chhod kar jump lagti hai, aur target ke paas aakar local track par utar jate hain ($O(\log N)$). B-Tree se zyada simple aur concurrent friendly hota hai.

### 9. Redis Pipelining & RTT (Round Trip Time)
- **Technical Definition:** Multiple commands ko bina har ek ke response ka wait kiye ek sath server par bhejna aur ek batch me response receive karna.
- **Analogy:** Tumne restaurant me waiter ko pehle bola "Paani lao", wo gaya aur laya (RTT 1). Fir bola "Roti lao", wo gaya aur laya (RTT 2). Pipelining matlab tumne ek hi baar me bol diya: "Paani, roti, sabzi, daal sab ek sath le aao" (Single RTT).

### 10. Watermarking & Bounded-Out-Of-Orderness (Flink)
- **Technical Definition:** Distributed streaming me network delay ki wajah se purani timestamp wali events late pahuchti hain. Watermark Flink ko batata hai ki "Humne timestamp $T$ tak ke saare events dekh liye hain, ab window close kardo."
- **Analogy:** School bus ka driver subah bolta hai: "Bus 8:00 AM par chalegi, lekin agar koi student 5 minute late hua toh hum 8:05 AM tak wait karenge." 8:05 AM par driver bus start kar deta hai (Watermark passed). Uske baad aane wale ko "late arrival" maana jayega.

### 11. RocksDB State Backend
- **Technical Definition:** Ek high-performance, embedded Key-Value database jo Log-Structured Merge (LSM) trees par chalta hai aur SSDs par fast writes allow karta hai.
- **Analogy:** Memory me sab kuch rakhne par agar data 500GB ho jaye toh RAM crash ho jayegi. RocksDB ek smart file cabinet ki tarah hai jo computer ki ultra-fast SSD me fit ho jata hai bina Java memory (GC) ko disturb kiye.

### 12. Index-Free Adjacency (Neo4j Graph Database)
- **Technical Definition:** Graph database me har node apne connected nodes ke physical memory address (pointer) ko direct store karta hai.
- **Analogy:** Relational DB me agar tumhe dhoondhna hai ki "Rahul ka dost kaun hai", toh tumhe pure desh ki voter list me 'Rahul' search karna padega ($O(\log N)$ index join). Index-Free Adjacency me Rahul ke hath me direct uske doston ke hath ki ungli pakdai hui hai; wo direct unhe kheench leta hai ($O(1)$ pointer traversal).

### 13. Multi-Tier Cache Hierarchy (L1 vs L2)
- **Technical Definition:** L1 cache application ke apne CPU/RAM heap me hoti hai (nanoseconds). L2 cache network par Redis server me hoti hai (milliseconds).
- **Analogy:** L1 cache tumhari jeb me rakha hua purse hai (turant nikal lo). L2 cache kamre me rakhi hui tijori hai (chabi dhoondh kar kholni padegi, thoda time lagega). Aur Database bank ka main locker hai (wahan jana sabse slow hai).

### 14. SIMD (Single Instruction, Multiple Data) & AVX-512
- **Technical Definition:** CPU ka hardware feature jo ek single clock cycle me 8 ya 16 alag-alag floating point numbers par parallel calculation kar deta hai.
- **Analogy:** Ek teacher 10 bacchon ki copy ek-ek karke check kare (Scalar processing). Ya fir teacher ek bada stamp le aur 10 bacchon ki copy ek sath line me rakh kar ek hi baar stamp maar de (SIMD vector processing).

### 15. Python GIL (Global Interpreter Lock)
- **Technical Definition:** CPython ka mutual exclusion lock jo ensure karta hai ki ek samay par sirf ek OS thread Python bytecode execute kare.
- **Analogy:** Ek kitchen me 4 cook khade hain, lekin pure kitchen me sirf **ek hi chammach** hai! Jiske hath me chammach hoga wahi khana banayega, baaki 3 hath bandh kar khade rahenge. (ONNX C++ engine me chammach ki zaroorat nahi padti, isiliye charo cook parallel kaam karte hain).

### 16. Dynamic Micro-Batching
- **Technical Definition:** High-throughput serving system me aane wali single-single requests ko short window (e.g. 5ms) ke liye collect karke ek tensor batch me model ko pass karna.
- **Analogy (Elevator / Lift):** Har ek aate hue bande ke liye lift ko upar-neeche le jana bewaqoofi hai. Lift darwaza 5 second ke liye khula rakhti hai taaki 6-7 log andar aa sakein, fir ek sath upar jati hai.

### 17. Class Imbalance & `scale_pos_weight`
- **Technical Definition:** Jab dataset me negative class (legitimate) 99.8% ho aur positive class (fraud) sirf 0.2% ho. `scale_pos_weight` fraud samples ke loss gradient ko multiply kar deta hai.
- **Analogy:** Ek teacher class ke 1,000 bacchon me se 2 chor pakadna chahta hai. Agar teacher aam galti par 1 daant lagata hai, toh chori pakadne par wo 500 guna bada fine lagata hai taaki model chori ko bilkul halke me na le.

### 18. PR-AUC vs. ROC-AUC Fallacy
- **Technical Definition:** Highly imbalanced data me ROC curve True Negatives ke huge count ki wajah se falsely optimistic picture dikhata hai. PR-AUC sirf Precision aur Recall par focus karta hai.
- **Analogy:** Ek doctor har aane wale patient ko bol de "Tumhe cancer nahi hai!" 1,000 me se 999 log normal the, toh doctor ki ROC-AUC accuracy 99.9% dikhegi. Lekin wo 1 cancer patient mar gaya! PR-AUC is doctor ko 0 score dega kyunki fraud detection me True Positives matter karte hain.

### 19. Cost-Sensitive $F_{0.5}$ Score Optimization
- **Technical Definition:** Ek evaluation metric jahan precision ko recall se zyada importance di jati hai ($\beta = 0.5$).
- **Analogy:** Agar tum police ho aur tumhe shaq hai. Tum 10 chor pakadne ke chakkar me 100 shareef nagrikon ko jail me nahi daal sakte (False Positive = customer churn). $F_{0.5}$ metric bolta hai: "Surety badhao! Jab 78% se zyada pakka shaq ho, tabhi arrest karo."

### 20. TreeSHAP (Shapley Additive Explanations)
- **Technical Definition:** Cooperative game theory par based algorithm jo mathematically batata hai ki prediction score ko badhane ya ghatane me har feature ka kitna contribution tha.
- **Analogy:** Ek cricket team match jeet gayi. SHAP value batati hai ki 300 runs me se Virat Kohli ka yogdaan 120 run tha, Rohit ka 80 run tha, aur bowlers ka 100 run tha. Aise hi model batata hai ki 0.89 fraud score me `impossible_travel` ka contribution +0.50 tha aur `amount_zscore` ka +0.25 tha.

### 21. Multi-Armed Bandit (MAB) & Exploration-Exploitation
- **Technical Definition:** Ek algorithm jo naye options ko explore karne (data collect karne) aur best known option ko exploit karne (maximum reward paane) ke beech balance banata hai.
- **Analogy:** Tum ek naye shahar me gaye. Wahan 5 restaurants hain. Kya tum roz usi 1 restaurant me khaoge jahan ka khana theek tha (Exploitation)? Ya naye restaurants try karoge shayad koi usse bhi behtar mil jaye (Exploration)?

### 22. Thompson Sampling & Beta-Bernoulli Prior
- **Technical Definition:** Bayesian probability distribution se probability parameter sample karke best arm choose karna.
- **Analogy:** Ek coin jo biased ho sakta hai. Tumne use 10 baar uchhala aur 7 baar Heads aaya. Tum 100% sure nahi ho ki agli baar Heads aayega, lekin tum ek mathematical bell curve (Beta distribution) banate ho aur us curve se ek number nikal kar bet lagate ho.

### 23. Shadow Model Deployment
- **Technical Definition:** Naye model ko production traffic receive karne dena, lekin uske output ko client ko na bhej kar sirf log karna validation ke liye.
- **Analogy:** Ek naya trainee pilot cockpit me baitha hai. Wo flight ke saare controls copy me note kar raha hai aur dekh raha hai ki wo kya karta, lekin actual plane senior captain hi uda raha hai. Jab trainee ki calculation 100% perfect sabit ho jati hai, tab use captain banaya jata hai.

### 24. Population Stability Index (PSI) & Jeffreys Divergence
- **Technical Definition:** Statistical metric jo do time periods ke beech probability distributions ke shift (data drift) ko measure karta hai using symmetric Kullback-Leibler divergence.
- **Analogy:** Agar pichhle saal tumhari dukan par aane wale 80% log 20-30 saal ke yuva the. Is mahine achanak 80% log 60+ saal ke senior citizens aane lage. Dukan wahi hai, lekin aane wali public badal gayi (Population Drift). PSI calculate karke bata dega ki distribution badal chuka hai.

### 25. Two-Sample Kolmogorov-Smirnov (KS) Test
- **Technical Definition:** Non-parametric statistical test jo do continuous empirical distributions ke maximum vertical separation ($D$) ko measure karta hai.
- **Analogy:** Do alag-alag school ke bacchon ke exam marks ka graph banao. Dono curves ke beech jahan sabse zyada gap hoga, wo distance $D$ hai. Agar wo gap ek limit se bada hai, toh dono school ka result statistically different hai.

### 26. Retraining Safety Rule
- **Technical Definition:** Production policy: Feature drift par model retrain karo, lekin Prediction score drift par model retrain KABHI mat karo (Security Alert raise karo).
- **Analogy:** Agar log achanak mehenge coat khareedne lage kyunki sardi aa gayi hai (Feature drift), toh dukan ka stock badal lo (Retrain). Lekin agar dukan me achanak 50 dacoit aa jayein aur samaan chori karne lagein (Active attack drift), toh dukan ke rules dacoit ke hisaab se mat badlo, balki police ko phone karo (SOC Security Alert)!

### 27. 64-bit Atomic Memory Pointer Swap
- **Technical Definition:** CPU hardware instruction jo single instruction cycle me memory reference ko replace kar deti hai bina kisi lock ya context switch ke.
- **Analogy:** Ek highway par sign-board laga hai "Go Left for Terminal 1". Ek worker ne 1 second ke fraction me us board ke aage sticker chipka diya "Go Right for Terminal 2". Koi gadi ruki nahi, koi traffic jam nahi hua, naya sign turant apply ho gaya.

### 28. Label Delay Survival Bias
- **Technical Definition:** Fraud ka actual ground-truth label aane me 14-60 din lagte hain. Recent data me fraud zero dikhta hai kyunki chargebacks abhi mature nahi huye.
- **Analogy:** Ek doctor ne dawai di aur 1 ghante baad bola "Mera ilaaj 100% successful hai kyunki kisi patient ne complain nahi ki!" Bhai, dawai ka asar hone aur side-effects aane me 2 hafte lagenge. 1 ghante baad report banana survival bias hai.

### 29. Circuit Breaker (Closed, Open, Half-Open)
- **Technical Definition:** Software design pattern jo cascading failures ko rokta hai jab downstream dependency fail hoti hai.
- **Analogy (Ghar ka MCB Switch):** Agar ghar me short-circuit ho jaye, toh wire jalne se pehle main board ka switch (MCB) gir jata hai (Circuit OPEN). Jab tum electrician ko bula kar fault theek kar lete ho, toh switch ko dheere se utha kar check karte ho (HALF-OPEN). Agar sab theek hai, toh normal light chalu rehti hai (CLOSED).

### 30. Coordinated Omission (Gil Tene's Theorem)
- **Technical Definition:** Load testing tool ka wo flaw jisme server ke pause hone par tool request bhejna band kar deta hai, jisse bad tail latencies report se gayab ho jati hain.
- **Analogy (Doctor's Waiting Room):** Doctor har 5 minute me 1 patient dekhta hai. Achanak doctor 1 ghante ke phone call par busy ho gaya. Bahar 12 patients ki line lag gayi aur sabka wait time 1 ghanta badh gaya. Lekin doctor ne diary me likha: "Maine toh phone ke baad wale patient ko 5 minute me hi dekha!" Is chupaye huye 1 ghante ke wait ko Coordinated Omission kehte hain.

---

## 8. Master Interview Cheatsheet (Rapid Verbal Defense)

Agar interview me cross-question aaye, toh in crisp Hindi-English points me confident jawab do:

### Q1: "Aapne sub-7ms P99 latency kaise achieve ki?"
> **Verbal Answer:** *"Humne critical hot path se saare blocking operations ko bahar nikal diya hai. Network ingress par Linux kernel ka `SO_REUSEPORT` 4 Gunicorn workers me zero-lock load distribution karta hai. Ingress parsing Pydantic v2 Rust SIMD se 0.25ms me hoti hai. Cache L1 in-process heap (5µs) aur L2 Redis pipelining (0.5ms) me split hai. ML inference ke liye hum Python runtime ko bypass karke C++ ONNX Runtime engine use karte hain jahan GIL release ho jata hai aur AVX-512 SIMD vectorization chalti hai. `ModelBatcher` 50 requests ko 1.5ms me micro-batch karta hai. Response serialization `orjson` Rust core se 18µs me hoti hai. Result: Average latency 3.9ms aur P99 latency **3.35ms**, well below our 7ms SLA."*

### Q2: "Kafka me per-user chronological ordering kaise guarantee hoti hai?"
> **Verbal Answer:** *"Kafka partitions scale karte hain, lekin agar ek user ke transactions alag-alag partitions par chale gaye toh sliding window velocity galat ho jayegi. Isiliye hamara Producer `Murmur2(user_id)` hash algorithm use karta hai. Is mathematical guarantee se ek specific user ki 100% transactions hamesha usi ek exact partition par land karti hain. Consumer side par `enable_auto_commit=False` ke sath At-Least-Once delivery semantics ensure hoti hai."*

### Q3: "Extreme class imbalance (0.2% fraud) ko kaise handle kiya?"
> **Verbal Answer:** *"Standard accuracy metric yahan useless hai kyunki 99.8% majority class hai. Humne XGBoost loss function me `scale_pos_weight = N_neg / N_pos = 499.0` inject kiya, jisse fraud sample miss hone par model ko 499x penalty lagti hai. Model evaluation ke liye humne ROC-AUC trap ko avoid karke Precision-Recall AUC (PR-AUC) use kiya. Threshold selection ke liye business cost ko dhyan me rakhte hue $F_{0.5}$ metric optimize kiya, jisne optimal threshold $\tau^* = 0.78$ calculate kiya."*

### Q4: "Production me naya model zero-downtime kaise deploy hota hai?"
> **Verbal Answer:** *"Hamara MLflow model registry container restart kiye bina models hot-reload karta hai. Serving process ke andar `ModelHandle` wrapper class hai. Jab naya ONNX model memory me load hota hai, hum Python me `self._current_session = new_session` execute karte hain. x86-64 CPU architecture par yeh ek single 64-bit atomic pointer swap instruction hai jo under 1 microsecond leti hai. Nayi requests instant naye model par switch ho jati hain aur in-flight requests gracefully purane pointer par finish hoti hain."*

### Q5: "Active fraud attack ke time model retrain kyu NAHI hona chahiye?"
> **Verbal Answer:** *"Yeh hamara Retraining Safety Rule hai. Agar coordinated bot attack chal raha hai, toh prediction score distribution me drift aayega (KS test fail hoga), lekin genuine feature distribution stable rahegi. Agar humne us waqt unlabelled attack data par automated retrain trigger kar diya, toh model fraud patterns ko normal behavior samajh kar memorize kar lega (Model Poisoning). Isiliye prediction drift par hum sirf Security Alert (SOC) raise karte hain. Retraining sirf tab trigger hoti hai jab feature drift ho ($\text{PSI} \ge 0.25$) aur matured ground-truth labels available hon."*

---

> **Document Status:** ✅ Completed & Authoritative Master Summary  
> **Source Files Verified:** `module1.md` through `module9.md`, `master_module.md`, `feature_columns.py`, `schemas.py`, `scoring.py`, `main.py`, `train.py`, `drift.py`, `circuit_breaker.py`  
> **Maintained by:** Real-Time Fraud Detection Engineering Team
