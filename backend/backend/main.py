import json
import os
from fastapi import FastAPI
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
import datetime

from confluent_kafka.cimpl import NewTopic, Producer
from backend.utils.forecasts import get_forecast

from backend.utils.raw_data import get_raw_data

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

@app.get("/forecast/{geohash}")
async def forecast(geohash):
    """
    function returns a forecast for a given geohash

    geohash - string of a geohash, must correspond to a geohash present in redis
    """
    return get_forecast(geohash)

@app.get("/rawdata/{timestamp}")
async def rawdata(timestamp=datetime.datetime.timestamp(datetime.datetime.utcnow() - datetime.timedelta(hours = 1))):
    """
    function returns all raw data from  timestamp up until now
    timestamp - unix timestamp in miliseconds, defaults to 1 hour
    """
    return get_raw_data(timestamp)