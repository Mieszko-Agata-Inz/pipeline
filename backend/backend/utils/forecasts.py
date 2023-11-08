import datetime
import json
import logging
import redis
from redis.commands.search.query import NumericFilter, Query
from backend.utils.cleanup import clean_aggregated
from backend.utils.conn import redisCli

def get_forecast(geohash, coldstart_models, hot_models):
    q = Query(f'@geohash:{geohash}').paging(0, 30).add_filter(
            NumericFilter(
                "timestamp",
                datetime.datetime.timestamp(datetime.datetime.utcnow()) - 300,
                datetime.datetime.timestamp(datetime.datetime.utcnow())
        ))
    #log1 = logging.getLogger("uvicorn.info")
    #log1.info("%s", "redis call", exc_info=1)
    res = redisCli.ft('aggregated').search(q)
    redisCli.quit()
    #log1.info("%s", "redis close", exc_info=1)
    
    if len(res.docs)==0:#change required number of dataframes here
        #cold goes here
        q2 = Query(f'@geohash:{geohash}').paging(0, 1).add_filter(
        NumericFilter(
            "timestamp",
            datetime.datetime.timestamp(datetime.datetime.utcnow()) - 10,
            datetime.datetime.timestamp(datetime.datetime.utcnow())
        )).sort_by("timestamp", asc=False)
        res = redisCli.ft('raw').search(q)
        response_json = json.loads(res.docs[0].json)
        fdate =  datetime.datetime.fromtimestamp(response_json["timestamp"])
        data = [
            response_json["humidity"],
            response_json["temp"],
            fdate.day,
            fdate.month,
            fdate.year,
            fdate.hour,
        ]
        #json.loads(res.docs[0].json)["temp"]
        dummy_data=[93.03,6.0,1,1,2015,4]#relh  sknt  day  month  year  hour
        val_1 = coldstart_models["xgb1"][1].predict([data])
        val_2 = coldstart_models["xgb2"][1].predict([data])
        val_3 = coldstart_models["xgb3"][1].predict([data])
        return [f"{val_1}", f"{val_2}", f"{val_3}"]
    #hot goes here
    return res