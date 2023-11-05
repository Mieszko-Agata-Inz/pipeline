import json
import logging
import os
from time import sleep
from fastapi import FastAPI, File, UploadFile
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
import datetime
from backend.utils.cleanup import clean_aggregated, clean_raw
from backend.utils.conn import redisCli
from confluent_kafka.cimpl import NewTopic, Producer
import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from backend.utils.forecasts import get_forecast
from fastapi_restful.tasks import repeat_every
from backend.utils.raw_data import get_raw_data
import pickle
#remove query_api to run with uvicorn - if not: query_api.utils.sample_weather

load_dotenv()
api_key="12345"
p = Producer({'bootstrap.servers': os.getenv('KAFKA_BROKER')})
coldstart_models = {
    "xgb1":("xgb1.pkl", pickle.load(open("backend/resources/xgb_1.pkl", "rb", -1))),
    "xgb2":("xgb2.pkl", pickle.load(open("backend/resources/xgb_2.pkl", "rb", -1))),
    "xgb3":("xgb3.pkl", pickle.load(open("backend/resources/xgb_3.pkl", "rb", -1))),
}
hot_models = {
    "test":("test", "test"),
}

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

@app.on_event("startup")
@repeat_every(seconds=40)
def remove_expired_tokens_task() -> None:
    clean_raw("dummy_timestamp")
    clean_aggregated()


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
    return get_forecast(geohash, coldstart_models, hot_models)

@app.get("/rawdata/{timestamp}")
async def rawdata(timestamp=datetime.datetime.timestamp(datetime.datetime.utcnow() - datetime.timedelta(hours = 1))):
    """
    function returns all raw data from  timestamp up until now
    timestamp - unix timestamp in miliseconds, defaults to 1 hour
    """
    log1 = logging.getLogger("uvicorn.info")
    log1.info("%s", "interesting problem", exc_info=1)
    return get_raw_data(timestamp)

@app.post("/update/{state}/{model_name}")
async def update_model(state: str, model_name: str, req_api_key: str, file: UploadFile = File(...)):
    """
    function returns all raw data from  timestamp up until now
    timestamp - unix timestamp in miliseconds, defaults to 1 hour
    """
    model_filename = f"{model_name}.pkl"
    model_path = f"backend/resources/{model_filename}"
    if req_api_key!=api_key:
        return False
    if state=="cold":
        if model_name in coldstart_models:
            try:
                contents = file.file.read()
                with open(model_path, 'wb') as f:
                    f.write(contents)
                coldstart_models[model_name] = (model_filename, pickle.load(open(model_path, "rb", -1)))
                return 1
            except Exception as e:
                return f"failed to load {e}" 
        return "model name not in coldstart models"
    if state=="hot":
        if model_name in hot_models:
            try:
                contents = file.file.read()
                with open(model_path, 'wb') as f:
                    f.write(contents)
                hot_models[model_name] = (model_filename, pickle.load(open(model_path, "rb", -1)))
                return 1
            except Exception as e:
                return (f"failed to load {e}")
        return "model name not in hot models"
    return "wrong model list"