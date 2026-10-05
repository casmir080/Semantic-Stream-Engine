import asyncio
import json
import logging
import httpx
import re
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HNProducer")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-events"

# Public Hacker News API endpoints (No Auth Required)
HN_MAX_ITEM_URL = "https://hacker-news.firebaseio.com/v0/maxitem.json"
HN_ITEM_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/{item_id}.json"

async def fetch_latest_item_id(client: httpx.AsyncClient) -> int | None:
    """Fetches the absolute newest item ID uploaded to Hacker News."""
    try:
        response = await client.get(HN_MAX_ITEM_URL, timeout=5.0)
        if response.status_code == 200:
            return int(response.text.strip())
    except Exception as e:
        logger.error(f"Error fetching max item ID: {e}")
    return None

async def fetch_item_details(client: httpx.AsyncClient, item_id: int) -> dict | None:
    """Retrieves metadata and text contents for a specific Hacker News item."""
    try:
        url = HN_ITEM_TEMPLATE.format(item_id=item_id)
        response = await client.get(url, timeout=5.0)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"Error fetching item details for {item_id}: {e}")
    return None

async def main():
    logger.info("Initializing Live Hacker News Ingestion Stream...")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Hacker News producer connected cleanly to Kafka broker.")

    last_processed_id = None

    async with httpx.AsyncClient() as http_client:
        try:
            # Seed the tracking loop on startup
            current_max_id = await fetch_latest_item_id(http_client)
            if current_max_id:
                # Instantly buffer the last 5 posts so the system starts processing immediately
                last_processed_id = current_max_id - 5  
                logger.info(f"Initialized tracking pointer at HN Item #{last_processed_id}")

            while True:
                new_max_id = await fetch_latest_item_id(http_client)
                
                if not new_max_id or not last_processed_id:
                    await asyncio.sleep(5)
                    continue

                if new_max_id > last_processed_id:
                    for target_id in range(last_processed_id + 1, new_max_id + 1):
                        item = await fetch_item_details(http_client, target_id)
                        
                        if not item:
                            continue

                        item_type = item.get("type", "unknown")
                        title = item.get("title", "")
                        text = item.get("text", "")
                        author = item.get("by", "anonymous")
                        
                        # Clean up raw comment tags often returned in raw HN JSON payloads
                        if text:
                            text = re.sub(r'<[^<]+?>', '', text)
                            text = text.replace("&quot;", '"').replace("&#x27;", "'").replace("&#x2F;", "/")

                        # Prioritize story titles or rich comment feedback blocks
                        content = title if title else text
                        if not content or len(content) < 15:
                            continue

                        payload = {
                            "text": content,
                            "source": "HackerNews",
                            "subreddit": f"HN-{item_type.upper()}",  # Map 'type' to 'subreddit' layout slot for UI alignment
                            "url": f"https://news.ycombinator.com/item?id={target_id}"
                        }

                        logger.info(f"📤 Pushed HN {item_type.capitalize()} by @{author}: {content[:60]}...")
                        
                        await producer.send_and_wait(
                            KAFKA_TOPIC,
                            json.dumps(payload).encode("utf-8")
                        )

                    last_processed_id = new_max_id

                # Poll Hacker News API index every 8 seconds for new public discussion drops
                await asyncio.sleep(8)

        except Exception as e:
            logger.error(f"Critical stream exception encountered in HN Ingestion: {e}")
        finally:
            logger.info("Cleaning up ingestion dependencies...")
            await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())