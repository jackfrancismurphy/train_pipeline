from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
import pprint

config_file = 'api-key-cc.txt'


connection = ConnectionConfig(
    bootstrap_servers="pkc-12576z.us-west2.gcp.confluent.cloud:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="3NQBFYHQKWFAAYLV",
    sasl_password="Zf0xVaHl1xYfkwVDmXcHJGxG1AqhuQRsqpac69R5tRdDd07BEdsg68WGONTpM8uo"
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
            # pprint.pp(f'Produce event with key="{kafka_msg.key}" value="{kafka_msg.value}"')
            producer.produce(
                topic=messages_topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )
