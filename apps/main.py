#!/usr/bin/env python3

# Standard
# import argparse

# Ignoring argument functionality for now because Trust is all I need, but do I need --durable? Left in just in case

import json
from time import sleep
import pprint
import logging
import threading

# Third party
import stomp

from kafka_producer import produce_message_confluent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Listener(stomp.ConnectionListener):
    _mq: stomp.Connection

    times = 1

    def __init__(self, mq: stomp.Connection, durable=False):
        self._mq = mq
        self.is_durable = durable

    def on_message(self, frame):

        headers, message_raw = frame.headers, frame.body
        message = json.loads(message_raw)
        

        if self.is_durable:
            # Acknowledging messages is important in client-individual mode
            self._mq.ack(id=headers["ack"],
                         subscription=headers["subscription"])

        for journey in message:
            logger.info(journey['body'])

        

        produce_message_confluent(message)


    def on_error(self, frame):
        print('received an error {}'.format(frame.body))

    def on_disconnected(self):
        print('disconnected')


if __name__ == "__main__":
    with open("../secrets.json") as f:
        secrets = json.load(f)

    feed_username, feed_password = secrets["feed_username_&_password"]

    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "-d",
    #     "--durable",
    #     action='store_true',
    #     help="Request a durable subscription. Note README before trying this.")
    # action = parser.add_mutually_exclusive_group(required=False)

    # args = parser.parse_args()

    # https://stomp.github.io/stomp-specification-1.2.html#Heart-beating
    # We're committing to sending and accepting heartbeats every 5000ms
    connection = stomp.Connection(
        [('publicdatafeeds.networkrail.co.uk', 61618)],
        keepalive=True,
        heartbeats=(10000, 10000))
    connection.set_listener('', Listener(connection))

    # Connect to feed
    connect_headers = {
        "username": feed_username,
        "passcode": feed_password,
        "wait": True,
    }
    # if args.durable:
    #     # The client-id header is part of the durable subscription - it should be unique to your account
    #     connect_headers["client-id"] = feed_username

    connection.connect(**connect_headers)

    topic = "/topic/TRAIN_MVT_ALL_TOC"

    # Subscription
    subscribe_headers = {
        "destination": topic,
        "id": 1,
    }
    # if args.durable:
    #     # Note that the subscription name should be unique both per connection and per queue
    #     subscribe_headers.update({
    #         "activemq.subscriptionName": feed_username + topic,
    #         "ack": "client-individual"
    #     })
    # else:
    #     subscribe_headers["ack"] = "auto"

    connection.subscribe(**subscribe_headers)

    while connection.is_connected():
        sleep(1)

    threading.Event().wait()
