import asyncio
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaProducer

# Set up basic logging so we can see what's happening in our terminal
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IngestionLayer")

app = FastAPI(title="SemanticStream Ingestion Engine")

# Configure Kafka connection details
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-events"

# We initialize the producer globally so FastAPI can manage its lifecycle
producer = None

@app.on_event("startup")
async def startup_event():
    """Triggered when FastAPI starts. We initialize and start our async Kafka producer here."""
    global producer
    logger.info("Starting Async Kafka Producer...")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Kafka Producer started successfully.")

@app.on_event("shutdown")
async def shutdown_event():
    """Triggered when FastAPI stops. Clean connections are happy connections."""
    global producer
    logger.info("Stopping Kafka Producer...")
    if producer:
        await producer.stop()
    logger.info("Kafka Producer stopped.")

@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint accepting live text streams.
    Ingests data and hands it off to Kafka instantly.
    """
    await websocket.accept()
    logger.info("New client connected to streaming pipeline.")
    
    try:
        while True:
            # Wait for incoming text data from the client
            data = await websocket.receive_text()
            
            # Prepare our payload
            payload = {
                "text": data,
                "timestamp": asyncio.get_event_loop().time() # Lightweight local monotonic timestamp
            }
            
            # Serialize to JSON string and encode to bytes for Kafka transmission
            serialized_payload = json.dumps(payload).encode("utf-8")
            
            # Asynchronously send to Kafka without blocking other WebSocket connections
            await producer.send_and_wait(KAFKA_TOPIC, serialized_payload)
            logger.info(f"Ingested and routed message to Kafka topic '{KAFKA_TOPIC}'")
            
    except WebSocketDisconnect:
        logger.info("Client disconnected normally from streaming pipeline.")
    except Exception as e:
        logger.error(f"Error in streaming connection: {str(e)}")