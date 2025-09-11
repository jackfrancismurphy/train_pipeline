from confluent_kafka import Consumer
import json
import logging
import signal
import sys
from flask import Flask, jsonify
from flask_cors import CORS

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Global variable to store the latest message
latest_data = None

# Load configuration
with open("../secrets.json", "r") as file:
    secrets = json.load(file)

# Configure Kafka consumer
conf = {
    'bootstrap.servers': secrets.get("bootstrap_servers"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': secrets.get("sasl_username"),
    'sasl.password': secrets.get("sasl_password"),
    'group.id': 'train-data-consumer',
    'auto.offset.reset': 'latest'
}

def signal_handler(signum, frame):
    """Handle shutdown gracefully"""
    logger.info("Shutting down consumer...")
    sys.exit(0)

@app.route('/api/latest', methods=['GET'])
def get_latest():
    """API endpoint to get the latest data"""
    return jsonify(latest_data)

def consume_messages():
    """Continuously consume messages from Kafka"""
    global latest_data
    
    # Create consumer instance
    consumer = Consumer(conf)
    
    try:
        # Subscribe to topic
        consumer.subscribe(['train_data_topic'])
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            elif msg.error():
                logger.error(f'Consumer error: {msg.error()}')
                continue
            
            try:
                # Parse and store the message
                value = json.loads(msg.value())
                latest_data = value
                logger.info(f"Received message: {value}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start the consumer in a separate thread
    from threading import Thread
    consumer_thread = Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    
    # Start Flask app
    app.run(host='0.0.0.0', port=5000)