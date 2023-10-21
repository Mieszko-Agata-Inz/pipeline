import datetime
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
    log1 = logging.getLogger("uvicorn.info")
    log1.info("%s", "redis call", exc_info=1)
    res = redisCli.ft('aggregated').search(q)
    redisCli.quit()
    log1.info("%s", "redis close", exc_info=1)
    dummy_data=[93.03,6.0,1,1,2015,4]#relh  sknt  day  month  year  hour
    #switch between models TODO
    val = coldstart_models["xgb1"][1].predict([dummy_data])
    return f"{val}"
    #do cleanup here
    #clean_aggregated()
    #make prediction here and return result
    return res