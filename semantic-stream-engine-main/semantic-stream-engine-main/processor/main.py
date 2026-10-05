import asyncio
import logging
import json
import time
import httpx
import asyncpg
from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ProcessorLayer")

# Database Connection String using your exact Postgres setup credentials
DB_DSN = "postgres://postgres:443302@localhost:5432/postgres"
OLLAMA_API_URL = "http://localhost:11434/api/generate"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-events"

# Unique isolated group ID preventing multi-terminal Kafka locks
DYNAMIC_GROUP_ID = f"semantic-processor-group-{int(time.time())}"

async def init_db():
    """Establishes connection to Postgres and ensures the target data schema exists."""
    logger.info("Connecting to PostgreSQL database...")
    conn = await asyncpg.connect(DB_DSN)
    
    # 1. Establish structural table matching your Hacker News streaming parameters
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS insights (
            id SERIAL PRIMARY KEY,
            raw_text TEXT NOT NULL,
            semantic_summary TEXT NOT NULL,
            sentiment VARCHAR(20) DEFAULT 'Neutral',
            subreddit VARCHAR(100),
            source_url TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    
    # Run structural database migrations cleanly so existing records are kept
    await conn.execute("ALTER TABLE insights ADD COLUMN IF NOT EXISTS sentiment VARCHAR(20) DEFAULT 'Neutral';")
    await conn.execute("ALTER TABLE insights ADD COLUMN IF NOT EXISTS subreddit VARCHAR(100);")
    await conn.execute("ALTER TABLE insights ADD COLUMN IF NOT EXISTS source_url TEXT;")
    
    logger.info("PostgreSQL table and schema validation complete.")
    return conn

async def analyze_text_with_llm(client: httpx.AsyncClient, text: str) -> dict | None:
    """Dispatches payload to local Ollama server enforcing a strict JSON response."""
    prompt = (
        f"Analyze this tech forum post. You must respond with a JSON object containing exactly two keys:\n"
        f"1. 'sentiment': must be exactly one of these strings: 'Positive', 'Negative', or 'Neutral'\n"
        f"2. 'summary': a clean, concise one-sentence summary of the user post.\n\n"
        f"Post contents: {text}"
    )
    
    ollama_payload = {
        "model": "llama3.2:3b",
        "prompt": prompt,
        "format": "json",  # Forces Ollama to strictly return a valid JSON structure
        "stream": False
    }
    
    try:
        response = await client.post(OLLAMA_API_URL, json=ollama_payload, timeout=60.0)
        response.raise_for_status()
        
        # Parse output string into structured local dictionaries
        raw_llm_response = response.json().get("response", "").strip()
        parsed_json = json.loads(raw_llm_response)
        
        return {
            "sentiment": parsed_json.get("sentiment", "Neutral").capitalize(),
            "summary": parsed_json.get("summary", "No summary generated.")
        }
    except Exception as e:
        logger.error(f"Inference engine failure or JSON parsing error: {e}")
        return None

async def main():
    # 1. Initialize persistent storage layer
    db_conn = await init_db()

    # 2. Configure Consumer with isolated dynamic group ID and timeout adjustments
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=DYNAMIC_GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",       # Pulls live data as it stream-broadcasts from Hacker News
        session_timeout_ms=90000,        
        heartbeat_interval_ms=30000,     
        max_poll_interval_ms=300000      
    )
    await consumer.start()
    logger.info(f"Consumer active with Isolated Group: {DYNAMIC_GROUP_ID}")
    logger.info(f"Listening to topic '{KAFKA_TOPIC}'...")

    try:
        async with httpx.AsyncClient() as http_client:
            while True:
                logger.info("🛋️ Waiting for incoming stream events...")
                msg = await consumer.getone() 
                logger.info("⚡ Message intercepted from Kafka! Processing...")

                try:
                    payload = json.loads(msg.value.decode('utf-8'))
                    raw_text = payload.get('text', '')
                    subreddit = payload.get('subreddit', 'HN-STORY')
                    source_url = payload.get('url', '')
                except Exception:
                    raw_text = msg.value.decode('utf-8')
                    subreddit = "HN-STORY"
                    source_url = ""

                logger.info(f"[Processing HN Event] -> Source: {subreddit} | Content: {raw_text[:50]}...")

                # Compute semantic analysis (Returns dict with keys: sentiment, summary)
                analysis = await analyze_text_with_llm(http_client, raw_text)
                
                if analysis:
                    summary = analysis["summary"]
                    sentiment = analysis["sentiment"]
                    logger.info(f"[Inference Complete] Tag: {sentiment} | Summary: {summary}")
                    
                    # Store data along with structural context keys 
                    await db_conn.execute(
                        '''
                        INSERT INTO insights (raw_text, semantic_summary, sentiment, subreddit, source_url) 
                        VALUES ($1, $2, $3, $4, $5)
                        ''', 
                        raw_text, summary, sentiment, subreddit, source_url
                    )
                    logger.info("✅ Record safely persisted to PostgreSQL database.")
                else:
                    logger.warning("Pipeline skipping save step due to empty or broken LLM evaluation.")
                    
    except Exception:
        logger.error("Fatal pipeline loop failure:", exc_info=True)
    finally:
        logger.info("Cleaning up pipeline dependencies...")
        await consumer.stop()
        await db_conn.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Pipeline stopped manually.")