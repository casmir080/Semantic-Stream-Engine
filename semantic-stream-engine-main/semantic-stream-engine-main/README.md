🧠 Semantic Stream Engine

A production-grade, end-to-end, event-driven AI pipeline that processes real-world data in real time. The system ingests live, high-intent technical discussions and stories from the Hacker News Live API, pipes them through Apache Kafka, executes local LLM inference using Llama 3.2 (3B) to extract structured semantic summaries and sentiments, and visualizes the analytical metadata on an auto-refreshing Streamlit dashboard.

🏗️ System Architecture

The project is designed as a modular, decoupled, event-driven microservices architecture:

┌───────────────────┐      ┌──────────────────┐      ┌────────────────┐
│  Hacker News API  ├─────►│  Async Producer  ├─────►│  Apache Kafka  │
│  (REST Ingestion) │      │   (Ingestion)    │      │ (Message Log)  │
└───────────────────┘      └──────────────────┘      └───────┬────────┘
                                                             │
┌───────────────────┐      ┌──────────────────┐              │
│   PostgreSQL DB   │◄─────┤  Async Consumer  │◄─────────────┘
│ (Analytical Store)│      │   (Processor)    │
└─────────┬─────────┘      └────────┬─────────┘
          │                         │
          │ (Read SQL)              │ (JSON Mode Inference)
          ▼                         ▼
┌───────────────────┐      ┌──────────────────┐
│Streamlit Dashboard│      │  Ollama Server   │
│(Live Visuals & UI)│      │  (Llama 3.2:3b)  │
└───────────────────┘      └──────────────────┘


Ingestion Tier (producer/main.py): An asynchronous, non-blocking Python worker that polls the public Hacker News stream, sanitizes raw comment payloads, structures them with metadata (type, author, origin URL), and dispatches them to a local Kafka topic.

Streaming Tier (Kafka Broker): Manages real-time log ingestion, distributed partitioning, and message offset management on port 9092.

AI Processing Tier (processor/main.py): An asynchronous consumer executing the core business logic. It reads from Kafka, hits a local Ollama server, handles state transitions, and writes insights cleanly to PostgreSQL.

Local Inference Engine (Ollama): Houses Llama 3.2 (3B) in strict JSON Mode, guaranteeing structural conformity for our analytics database schema.

Serving Tier (PostgreSQL): Persists raw content, structured summaries, categorizations, and temporal indicators securely on port 5432.

Analytics UI (dashboard/app.py): A Streamlit interface displaying real-time metrics, sentiment market share, and an live interactive data table.

🛠️ Key Engineering Challenges Solved

Building real-time AI pipelines requires addressing significant friction points between high-velocity message queues and high-latency LLM inference tasks. Here is how this project solves those hurdles:

1. Downstream Backpressure & Consumer Eviction Loops

The Problem: Local LLM inference on consumer-grade hardware is slow, taking anywhere from 5 to 40 seconds depending on payload size. Because our consumer took too long to complete its work and call the next poll loop, the Kafka broker assumed the consumer was dead and evicted it, triggering continuous rebalance storms.

The Fix: Tuned the consumer configurations defensively:

Increased session_timeout_ms to 90000 (90 seconds).

Maintained a consistent background tick rate with heartbeat_interval_ms set to 30000 (30 seconds).

Expanded max_poll_interval_ms to 300000 (5 minutes), allowing the LLM ample processing headroom without triggering eviction.

2. Windows-Specific Async Stalling

The Problem: In Python 3.12 running on Windows systems, the standard async for msg in consumer block silently freezes due to loop-handling bugs within aiokafka.

The Fix: Bypassed the high-level loop structures and implemented a direct, lower-level loop polling cycle utilizing explicit await consumer.getone() operations.

3. Stale Rebalance Bottlenecks

The Problem: Developing and testing streaming workers often results in frequent restarts. Standard Kafka configurations hold partition locks for several minutes on a shutdown/restart cycle, causing long debugging delays.

The Fix: Programmed timestamp-based dynamic group_id assignments (semantic-processor-group-{timestamp}) to instantly hot-swap consumer connection states.

4. Robust Database-State Resiliency

The Problem: Null or empty fields from older databases or historical test structures can cause Pandas schema parsers to crash, throwing non-iterable type errors on the dashboard.

The Fix: Engineered defensive cleaning layers inside the visualizer, utilizing .fillna() and explicit type conversion (.astype(str)) to securely map metadata columns, preventing UI runtime failures.

🚀 Getting Started

Prerequisites

Docker & Docker Compose

Python 3.11+

Ollama installed locally

Step 1: Set Up Infrastructure

Spin up Kafka and PostgreSQL in the background:

docker-compose up -d


Ensure Ollama has Llama 3.2 ready:

ollama pull llama3.2:3b


Step 2: Set Up Python Virtual Environment

Initialize, activate, and install dependencies:

python -m venv processor/venv
# Windows PowerShell:
.\processor\venv\Scripts\Activate.ps1
# Mac/Linux:
source processor/venv/bin/activate

pip install aiokafka asyncpg httpx streamlit pandas praw


Step 3: Run the Pipeline (Open 3 Terminals)

Terminal 1: Start the AI Processor (initializes tables and monitors Kafka)

python processor/main.py


Terminal 2: Start the Hacker News Producer (begins real-time ingestion)

python producer/main.py


Terminal 3: Launch the Analytics Dashboard

streamlit run dashboard/app.py


📁 Repository Directory Structure

semantic-stream-engine/
├── docker-compose.yml   # Kafka & Postgres local containers
├── README.md            # Documentation
├── producer/
│   └── main.py          # HN API Live Producer (Async)
├── processor/
│   └── main.py          # Llama 3.2 Inference Processor (Async)
└── dashboard/
    └── app.py           # Streamlit Analytics Dashboard UI


📜 Database Schema

CREATE TABLE IF NOT EXISTS insights (
    id SERIAL PRIMARY KEY,
    raw_text TEXT NOT NULL,
    semantic_summary TEXT NOT NULL,
    sentiment VARCHAR(20) DEFAULT 'Neutral',
    subreddit VARCHAR(100), -- Maps directly to HN Event Type
    source_url TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
