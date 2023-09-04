import json
import os
from fastapi import FastAPI
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

from confluent_kafka.cimpl import NewTopic, Producer

#remove query_api to run with uvicorn - if not: query_api.utils.sample_weather

load_dotenv()

p = Producer({'bootstrap.servers': os.getenv('KAFKA_BROKER')})

app = FastAPI()

# @app.on_event('startup')
# async def api_query_startup():
#     a = AdminClient({'bootstrap.servers': os.getenv('KAFKA_BROKER')})
#     topic_list=[]
#     topic_list.append(NewTopic(topic="weather_data", num_partitions=1, replication_factor=1))
#     a.create_topics(new_topics=topic_list, validate_only=False)


@app.get("/")
async def root():
    return {"message": f"No datapoints available"}

