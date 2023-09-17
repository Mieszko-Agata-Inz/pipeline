import redis
# import os
import time
import json
from kafka import KafkaConsumer

time.sleep(40)

## with JSON

# data = {
#     'geohash': {
#         'long' : '49',
#         'lat' : '22'
#     }
# }

# redisCli = redis.Redis(
#     host='localhost',
#     port=6379,
#     charset="utf-8",
#     decode_responses=True,
#     password = "eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81" #os.getenv('REDIS_PASSWORD')
#     )


# writing    
# redisCli.json().set('doc', '$', data)

# reading
# doc = redisCli.json().get('doc', '$')
# dog = redisCli.json().get('doc', '$.dog')
# longtitude = redisCli.json().get('doc', '$..long')
# print(doc, dog, longtitude)

## NOT JSON

# open redis
redisCli = redis.Redis(
    host='redis',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    password = "eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81"
    )
# kafka consumer 
n=0
kafka_consumer = KafkaConsumer('weather_data_output', bootstrap_servers='kafka:29092')
kafka_consumer_2 = KafkaConsumer('raw_weather_data', bootstrap_servers='kafka:29092')

# for message in kafka_consumer: redis-cli -a 'eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81'
#     data = message.value
#     key = message.key
#     print(data)
#     value = redisCli.set(key, data)

def process_summary(message):
    global n
    print(message)
    for key, value in message.items():
        for consumer_record in value:
            redisCli.json().set(f"s{key}", '$',(consumer_record.value.decode('utf-8')))
            n+=1


def process_raw(message):
    global n
    print(message)
    for key, value in message.items():
        for consumer_record in value:
            redisCli.json().set(f"r{n}", '$', json.dumps(consumer_record.value.decode('utf-8')))
            n+=1

timeout = 100
while True:
    message1 = kafka_consumer.poll(timeout_ms=timeout)
    if message1:
        process_summary(message1)


    message2 = kafka_consumer_2.poll(timeout_ms=timeout)
    if message2:
        process_raw(message2)


# writing
# value = redisCli.set("key0", "value0")
#reading

# valueout = redisCli.get("key0")
# print(valueout)



