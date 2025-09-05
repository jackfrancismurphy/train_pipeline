from confluent_kafka import Consumer, TopicPartition
import json
import logging
import time  # Import time module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("../secrets.json", "r") as file:
    secrets = json.load(file)

# Configure Confluent Kafka consumer
conf = {
    'bootstrap.servers': secrets.get("bootstrap_servers"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': secrets.get("sasl_username"),
    'sasl.password': secrets.get("sasl_password"),
    'group.id': 'train-data-reader',
    'auto.offset.reset': 'latest'
}

def consume_one_message_confluent():
    consumer = Consumer(conf)
    try:
        consumer.subscribe(['train_data_topic'])
        
        # Wait for assignment
        while not consumer.assignment():
            consumer.poll(0.1)
            
        # Getting the most recent message is tricky, there is no "out of the box" solution for it

        # Get the last message from each partition
        for partition in consumer.assignment():
            low, high = consumer.get_watermark_offsets(partition)
            if high > 0:
                # Seek to last message
                consumer.seek(TopicPartition(partition.topic, partition.partition, high - 1))
                
        # Poll for the message
        msg = consumer.poll(5.0)
        
        if msg is None:
            logger.info("No messages found in topic")
            return None
        elif msg.error():
            logger.error('Kafka error: %s', msg.error())
            return None
        else:
            try:
                value = json.loads(msg.value()) if msg.value() else None
                logger.info("Last message: %s", value)
                return value
            except Exception as e:
                logger.error("Error processing message: %s", e)
                return None
    finally:
        consumer.close()

if __name__ == "__main__":
    result = consume_one_message_confluent()
    if result:
        print("\n=== Most Recent Message ===")
        print(json.dumps(result, indent=2))