import json
import logging
import os
from time import sleep
from fastapi import FastAPI
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
import datetime
from backend.utils.conn import redisCli
from confluent_kafka.cimpl import NewTopic, Producer
import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from backend.utils.forecasts import get_forecast

from backend.utils.raw_data import get_raw_data

#remove query_api to run with uvicorn - if not: query_api.utils.sample_weather

load_dotenv()

p = Producer({'bootstrap.servers': os.getenv('KAFKA_BROKER')})

app = FastAPI()



@app.on_event('startup')
async def initialize_index():
    sleep(20)
    schema_raw = (
        NumericField("$.lat", as_name="latitude"),
        NumericField("$.long", as_name="longtitude"),
        NumericField("$.timestamp", as_name="timestamp"),
        TextField("$.geohash", as_name="geohash")
    )
    schema_processed = (
        NumericField("$.timestamp", as_name="timestamp"),
        TextField("$.geohash", as_name="geohash")
    )
    redisCli.ft('raw').create_index(schema_raw, definition=IndexDefinition(prefix=["raw:"], index_type=IndexType.JSON))
    redisCli.ft('aggregated').create_index(schema_processed, definition=IndexDefinition(prefix=["summ:"], index_type=IndexType.JSON))
    redisCli.quit()
    print("init success")


#redis-cli -a "eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81"
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
    log1 = logging.getLogger("uvicorn.info")
    log1.info("%s", "interesting problem", exc_info=1)
    return get_raw_data(timestamp)