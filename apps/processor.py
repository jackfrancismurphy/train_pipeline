from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
import json
import pprint
from kafka_producer import connection

how_many = [0]

registered_journeys = []

delayed_train_leaderboard = {}

with open("..\\data_retrieval\\company_names.json") as company_names:
    company_lookup = json.load(company_names)

# move both of these into one file ??

with open("../secrets.json", "r") as file:
    secrets = json.load(file)

connection = ConnectionConfig(
    bootstrap_servers=secrets.get("bootstrap_servers"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username=secrets.get("sasl_username"),
    sasl_password=secrets.get("sasl_password")
)



# connect to your local Kafka broker
#
app = Application(
    broker_address=connection,
    consumer_group="consume-v1",
    auto_offset_reset="earliest"
)

# change the argument given to the lambda so that we already have access to
# body

def process_data(row, registered_journeys, delayed_train_leaderboard):


    train_entry = row[0]['body']

    train_entry["toc_id"] = company_lookup[train_entry["toc_id"]]

    if train_entry["train_id"] not in registered_journeys:
        registered_journeys.append(train_entry["train_id"])

        delayed_train_leaderboard = update_leaderboard(row, train_entry, delayed_train_leaderboard)

    # if delayed_train_leaderboard:
    #     print(delayed_train_leaderboard)


    # Process data NOW returns the leaderboard to the top level. How do I update the global variable? Row 97
    # Does a train with the same train id do journeys later on? 





def update_leaderboard(row, train, leaderboard):

    try:
        if train["variation_status"] == "LATE" and train["toc_id"] not in leaderboard:
            leaderboard[train["toc_id"]] = 1
            print(leaderboard)


        elif train["variation_status"] == "LATE" and train["toc_id"] in leaderboard:
            leaderboard[train["toc_id"]] += 1
            print(leaderboard)

    except KeyError:
        pass
        # variation_status does not exist in the dictionary






    # This is the function which, for each new train registered as late,
    # adds or updates the delayed_train_leaderboard with each new
    # late train for each company





# configure the input topic to subscribe to (you'll read data from this topic)
input_topic = app.topic("train_data_topic")

# consume (read) messages from the input topic
sdf = app.dataframe(topic=input_topic)

# print every row
sdf = sdf.update(lambda row: process_data(row, registered_journeys, delayed_train_leaderboard))

if __name__ == "__main__":
    # run the application and process all inbound messages using the sdf pipeline
    app.run(sdf)
