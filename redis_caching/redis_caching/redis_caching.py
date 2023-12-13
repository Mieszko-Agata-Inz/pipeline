import redis

# import os
import time
import json
from kafka import KafkaConsumer

time.sleep(40)

## with JSON
# open redis
redisCli = redis.Redis(
    host="redis",
    port=6379,
    charset="utf-8",
    decode_responses=True,
    password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81",
)
# kafka consumer
nr = 0
ns = 0
kafka_consumer = KafkaConsumer("weather_data_output", bootstrap_servers="kafka:29092")
kafka_consumer_2 = KafkaConsumer("raw_weather_data", bootstrap_servers="kafka:29092")


def process_summary(message):
    global ns
    print(message)
    for key, value in message.items():
        for consumer_record in value:
            data = json.loads(
                consumer_record.value
            )  # .decode(encoding="ascii", errors="replace")
            data["geohash"] = consumer_record.key.decode("ascii")
            data["timestamp"] = int(data["timestamp"])
            try:
                redisCli.json().set(f"summ:{ns}", "$", data)
                redisCli.quit()
            except:
                print("connection reset")
            ns += 1


def process_raw(message):
    global nr
    print(message)
    for key, value in message.items():
        for consumer_record in value:
            data = json.loads(
                consumer_record.value
            )  # .decode(encoding="ascii", errors="replace")
            print(consumer_record.key.decode("ascii"))  # new - delete later
            data["geohash"] = consumer_record.key.decode("ascii")
            data["timestamp"] = int(data["timestamp"])
            try:
                redisCli.json().set(f"raw:{nr}", "$", data)
                redisCli.quit()
            except:
                print("connection reset")
            nr += 1


timeout = 100
while True:
    message1 = kafka_consumer.poll(timeout_ms=timeout)
    if message1:
        process_summary(message1)

    message2 = kafka_consumer_2.poll(timeout_ms=timeout)
    if message2:
        process_raw(message2)
