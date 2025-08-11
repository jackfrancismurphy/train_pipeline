from quixstreams import Application
from quixstreams.kafka import ConnectionConfig
import pprint
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("../secrets.json", "r") as file:
    secrets = json.load(file)

connection = ConnectionConfig(
    bootstrap_servers=secrets.get("bootstrap_servers"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username=secrets.get("sasl_username"),
    sasl_password=secrets.get("sasl_password")
)

app = Application(broker_address=connection)


# Define a topic with chat messages in JSON format
messages_topic = app.topic(name="train_data_topic", value_serializer="json")

messages = [
    {"user_message":"test message"}
]


def main():
    with app.get_producer() as producer:
        for message in messages:
            # Serialize chat message to send it to Kafka
            # Use "chat_id" as a Kafka message key
            kafka_msg = messages_topic.serialize(key=None, value=message)

            # Produce chat message to the topic
            pprint.pp(f'Produce event with key="{kafka_msg.key}" value="{kafka_msg.value}"')
            producer.produce(
                topic=messages_topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )


def produce_message_confluent(message):
    with app.get_producer() as producer:
        while message:
            # Serialize chat message to send it to Kafka
            # Use "chat_id" as a Kafka message key
            kafka_msg = messages_topic.serialize(key=None, value=message)

            # Produce chat message to the topic
            logger.info(f'Produce event with key="{kafka_msg.key}" value="{kafka_msg.value}"')
            producer.produce(
                topic=messages_topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )
