#!/usr/bin/env python3

# Standard
# import argparse

# Ignoring argument functionality for now because Trust is all I need, but do I need --durable? Left in just in case

import json
import sys
import os
from time import sleep
import pprint as pp
import logging
import threading
import stomp
from datetime import date

# Local files
from kafka_producer import produce_message_confluent
from consuming_by_date import consume_one_message_confluent

STATE_FILE = "..\\app_state.json"

registered_journeys = []
allow_message = False

registered_journeys = []

delayed_train_leaderboard = {}

allow_message = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("..\\data_retrieval\\company_names.json") as company_names:
    company_lookup = json.load(company_names)

most_recent_message = consume_one_message_confluent()

if most_recent_message == None or most_recent_message["date"] == str(date.today()):
    delayed_train_leaderboard = {"date": str(date.today()), "leaderboard": {}}


elif most_recent_message["date"] == str(date.today()):
    delayed_train_leaderboard = consume_one_message_confluent()


# print(delayed_train_leaderboard)

def do_dates_differ():
    today = str(date.today())

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        last_run = state.get("last_run_date")
    else:
        last_run = None

    dates_differ = (last_run != today)

    return dates_differ



def update_leaderboard(train, delayed_train_leaderboard):

    leaderboard = delayed_train_leaderboard["leaderboard"]
    toc_id = train["toc_id"]

    if toc_id not in leaderboard:
        leaderboard[train["toc_id"]] = 1
        pp.pprint(leaderboard)


    elif toc_id in leaderboard:
        leaderboard[train["toc_id"]] += 1
        pp.pprint(leaderboard)


# Has it crashed? Is it the first time the program has run today? Either load today's leaderboard or reinitialise it

# Testing that functionality

# first_run_bool, delayed_train_leaderboard = is_first_run_today(delayed_train_leaderboard)


# if os.path.exists(STATE_FILE):
#     with open(STATE_FILE, "r") as f:
#         state = json.load(f)
#     print(state.get("last_run_date"))
# print("\n")
# print(str(date.today()))
# print("\n")
# print(first_run_bool)

# if first_run_bool == True:
#     delayed_train_leaderboard = {"date": date.today(), "leaderboard": {}}

# elif first_run_bool == False:





class Listener(stomp.ConnectionListener):
    _mq: stomp.Connection

    def __init__(self, mq: stomp.Connection, delayed_train_leaderboard, durable=False):
        self._mq = mq
        self.delayed_train_leaderboard = delayed_train_leaderboard
        self.is_durable = durable

    def on_message(self, frame):

        if do_dates_differ() == True:
            delayed_train_leaderboard = {"date": str(date.today()), "leaderboard": {}}

            #update the file to the new date

            try:
                with open(STATE_FILE, "r") as f:
                    state = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                state = {}

            # 2. Overwrite the value of 'last_run_date'
            state["last_run_date"] = date.today().isoformat()

            # 3. Write the entire updated dictionary back to the file
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=4)


        headers, message_raw = frame.headers, frame.body
        message = json.loads(message_raw)


        if self.is_durable:
            # Acknowledging messages is important in client-individual mode
            self._mq.ack(id=headers["ack"],
                         subscription=headers["subscription"])
        

        # Start processing logic
        
        # Presume that a new message doesn't contain a new delay
        allow_message = False

        train_entry = message[0]['body']

        try: 
            if train_entry["train_id"] not in registered_journeys and train_entry["variation_status"] == "LATE":

                registered_journeys.append(train_entry["train_id"]) 

                train_entry["toc_id"] = company_lookup[train_entry["toc_id"]]       

                # this is also where the leaderboard is updated, alongside updating the allow_message variable
                update_leaderboard(train_entry, self.delayed_train_leaderboard)

                allow_message = True

        except KeyError:
            # sometimes "variation status" doesn't exist
            pass
            

        if allow_message == True:

            produce_message_confluent(self.delayed_train_leaderboard)


    def on_error(self, frame):
        print('received an error {}'.format(frame.body))

    def on_disconnected(self):
        print(f"disconnected at {date.datetime.now()}")



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
    # We're committing to sending and accepting heartbeats every 100000ms
    connection = stomp.Connection(
        [('publicdatafeeds.networkrail.co.uk', 61618)],
        keepalive=True,
        heartbeats=(100000, 100000))
    connection.set_listener('', Listener(connection, delayed_train_leaderboard))

    # Connect to feed
    connect_headers = {
        "username": feed_username,
        "passcode": feed_password,
        "wait": True,
    }
    # if args.durable:
    #     # The client-id header is part of the durable subscription - it should be unique to your account
    #     connect_headers["client-id"] = feed_username

    topic = "/topic/TRAIN_MVT_ALL_TOC"
    attempt = 1

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

    while True:
            try: 
                connection.connect(**connect_headers)
                connection.subscribe(**subscribe_headers)
                break
            except Exception as e:
                logger.error(f"Failed attempt {attempt}: {e}")
                wait_time = min(20, 2 ** attempt)
                sleep(wait_time)

    while connection.is_connected():
        sleep(1)

    threading.Event().wait()



"""
If you want to make it cleaner you can:
move initialization logic inside listener.__init__
Integrate do_dates_differ() more tightly
"""