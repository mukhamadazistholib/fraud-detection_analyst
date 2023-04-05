from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
import os

def load_avro_schema_from_file():
    key_schema = avro.load("key_schema.avsc")
    value_schema = avro.load("value_schema.avsc")

    return key_schema, value_schema


def send_record(file_path):
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    file = open(file_path)
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        key = {"step":  str(row[0])}
        value = {"step": str(row[0]), "type": str(row[1]), "amount": float(row[2]), "nameOrig": str(row[3]), "oldbalanceOrg": float(row[4]), "newbalanceOrig": float(row[5]), "nameDest": str(row[6]), "oldbalanceDest": float(row[7]), "newbalanceDest": float(row[8]), "isFraud": str(row[9]), "isFlaggedFraud": str(row[10])}

        try:
            producer.produce(topic='fraud', key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        # sleep(1) 

    file.close()

if __name__ == "__main__":
    directory = 'data'
    for filename in os.listdir(directory):
        # if '%log%' in filename and filename.endswith(".csv"):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            send_record(file_path)
