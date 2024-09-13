from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
import json
from kafka_producer import connection

how_many = [0]

registered_journeys = []

delayed_train_leaderboard = {}

with open("..\\data_retrieval\\company_names.json") as company_names:
    company_lookup = json.load(company_names)

connection = ConnectionConfig(
    bootstrap_servers="pkc-12576z.us-west2.gcp.confluent.cloud:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="3NQBFYHQKWFAAYLV",
    sasl_password="Zf0xVaHl1xYfkwVDmXcHJGxG1AqhuQRsqpac69R5tRdDd07BEdsg68WGONTpM8uo"
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

    if delayed_train_leaderboard:
        print(delayed_train_leaderboard)


    # Process data NOW returns the leaderboard to the top level. How do I update the global variable? Row 97





def update_leaderboard(row, train, leaderboard):
    
    if train["variation_status"] == "LATE" and train["toc_id"] not in leaderboard:
        leaderboard[train["toc_id"]] = 1
    
    elif train["variation_status"] == "LATE" and train["toc_id"] in leaderboard:
        leaderboard[train["toc_id"]] += 1
        
    


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