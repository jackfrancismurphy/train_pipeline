import psycopg2
import json


with open("../secrets.json", "r") as file:
    secrets = json.load(file)


hostname = secrets["aiven_hostname"]
port = secrets["port"]
database = secrets["database"]
username = secrets["database_username"]
password = secrets["database_password"]

# SQL statement to create the table
create_table_query = """
CREATE TABLE train_events (
    train_id VARCHAR(20),
    actual_timestamp BIGINT,
    loc_stanox VARCHAR(10),
    gbtt_timestamp BIGINT,
    planned_timestamp BIGINT,
    planned_event_type VARCHAR(20),
    event_type VARCHAR(20),
    event_source VARCHAR(20),
    correction_ind BOOLEAN,
    offroute_ind BOOLEAN,
    direction_ind VARCHAR(5),
    platform VARCHAR(5),
    route VARCHAR(10),
    train_service_code VARCHAR(20),
    division_code VARCHAR(10),
    toc_id VARCHAR(10),
    timetable_variation INT,
    variation_status VARCHAR(10),
    next_report_stanox VARCHAR(10),
    next_report_run_time INT,
    train_terminated BOOLEAN,
    delay_monitoring_point BOOLEAN,
    reporting_stanox VARCHAR(10),
    auto_expected BOOLEAN
);
"""

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=hostname,
        port=port,
        dbname=database,
        user=username,
        password=password
    )
    cursor = conn.cursor()

    # Execute the create table query
    cursor.execute(create_table_query)
    conn.commit()

    print("Table 'train_events' created successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
