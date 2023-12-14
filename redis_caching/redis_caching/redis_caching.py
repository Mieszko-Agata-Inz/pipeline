import redis
import time
import json
from kafka import KafkaConsumer

# Delay execution for 40 seconds
time.sleep(40)

# Initialize Redis client with connection parameters
redisCli = redis.Redis(
    host="redis",
    port=6379,
    charset="utf-8",
    decode_responses=True,
    password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81",
)

# Initializing counters for processed messages
nr = 0  # Counter for raw messages
ns = 0  # Counter for summary messages

# Creating Kafka consumers for two topics: 'weather_data_output' and 'raw_weather_data'
kafka_consumer = KafkaConsumer("weather_data_output", bootstrap_servers="kafka:29092")
kafka_consumer_2 = KafkaConsumer("raw_weather_data", bootstrap_servers="kafka:29092")

# Function to process summary messages from Kafka
def process_summary(message):
    global ns
    print(message)
    for key, value in message.items():
        for consumer_record in value:
            # Deserialize JSON data from message
            data = json.loads(consumer_record.value)
            # Extract and add geohash and timestamp to data
            data["geohash"] = consumer_record.key.decode("ascii")
            data["timestamp"] = int(data["timestamp"])
            try:
                # Store data in Redis using JSON set, with a unique key 'summ:{ns}'
                redisCli.json().set(f"summ:{ns}", "$", data)
                # Disconnect from Redis - might not be optimal for repeated use
                redisCli.quit()
            except:
                print("connection reset")
            ns += 1  # Increment counter for each processed message

# Function to process raw messages from Kafka
def process_raw(message):
    global nr
    print(message)
    for key, value in message.items():
        for consumer_record in value:
            # Deserialize JSON data from message
            data = json.loads(consumer_record.value)
            # Extract and add geohash and timestamp to data
            data["geohash"] = consumer_record.key.decode("ascii")
            data["timestamp"] = int(data["timestamp"])
            try:
                # Store data in Redis using JSON set, with a unique key 'raw:{nr}'
                redisCli.json().set(f"raw:{nr}", "$", data)
                # Disconnect from Redis - might not be optimal for repeated use
                redisCli.quit()
            except:
                print("connection reset")
            nr += 1  # Increment counter for each processed message

# Timeout for polling messages from Kafka
timeout = 100

# Infinite loop to continuously poll and process messages from both Kafka consumers
while True:
    message1 = kafka_consumer.poll(timeout_ms=timeout)
    if message1:
        process_summary(message1)

    message2 = kafka_consumer_2.poll(timeout_ms=timeout)
    if message2:
        process_raw(message2)
