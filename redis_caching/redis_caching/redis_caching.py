import redis
# import os
import time

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
kafka_consumer = KafkaConsumer('weather_data_output', bootstrap_servers='kafka:29092')

for message in kafka_consumer:
    data = message.value
    key = message.key
    print(data)
    value = redisCli.set(key, data)


# writing
# value = redisCli.set("key0", "value0")
#reading

# valueout = redisCli.get("key0")
# print(valueout)