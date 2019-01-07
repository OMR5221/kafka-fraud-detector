"""Produce fake transactions into a Kafka topic."""
import threading, logging, time
import multiprocessing
import os
from time import sleep
import json

from kafka import KafkaProducer
from transactions import create_random_transaction

TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

class Producer(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            # Encode all values as JSON
            value_serializer=lambda value: json.dumps(value).encode(),
        )
        while not self.stop_event.is_set():
            transaction: dict = create_random_transaction()
            producer.send(TRANSACTIONS_TOPIC, value=transaction)
            print(transaction) # DEBUG
            time.sleep(1)

        producer.close()

def main():
    tasks = [
        Producer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()

if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
