import datetime
import json
import logging
import redis
import numpy
from redis.commands.search.query import NumericFilter, Query
from backend.utils.cleanup import clean_aggregated
from backend.utils.conn import redisCli


def get_forecast(geohash, coldstart_models, hot_models):
    # window_size and features_size: depends on hot model
    window_size = 6
    features_size = 3

    q = (
        Query(f"@geohash:{geohash}")
        .paging(0, 30)
        .add_filter(
            NumericFilter(
                "timestamp",
                datetime.datetime.timestamp(datetime.datetime.utcnow()) - 300,
                datetime.datetime.timestamp(datetime.datetime.utcnow()),
            )
        )
        .sort_by("timestamp", asc=False)
    )
    # log1 = logging.getLogger("uvicorn.info")
    # log1.info("%s", "redis call", exc_info=1)
    res = redisCli.ft("aggregated").search(q)

    # log1.info("%s", "redis close", exc_info=1)
    print(f"recieved {len(res.docs)} aggregated")
    if len(res.docs) < window_size:  # change required number of dataframes here
        # cold goes here
        q2 = (
            Query(f"@geohash:{geohash}")
            .paging(0, 1)
            .add_filter(
                NumericFilter(
                    "timestamp",
                    datetime.datetime.timestamp(datetime.datetime.utcnow()) - 10,
                    datetime.datetime.timestamp(datetime.datetime.utcnow()),
                )
            )
            .sort_by("timestamp", asc=False)
        )
        res = redisCli.ft("raw").search(q)
        print(f"recieved {len(res.docs)} raw")
        if len(res.docs) == 0:
            return "no data"
        response_json = json.loads(res.docs[0].json)
        fdate = datetime.datetime.fromtimestamp(response_json["timestamp"])
        data = [
            response_json["humidity"],
            response_json["temp"],
            fdate.day,
            fdate.month,
            fdate.year,
            fdate.hour,
        ]
        # json.loads(res.docs[0].json)["temp"]
        #dummy_data = [93.03, 6.0, 1, 1, 2015, 4]  # relh  sknt  day  month  year  hour
        val_1 = coldstart_models["xgb1"][1].predict([data])
        val_2 = coldstart_models["xgb2"][1].predict([data])
        val_3 = coldstart_models["xgb3"][1].predict([data])
        return ["xgb", f"{val_1}", f"{val_2}", f"{val_3}"]
    # so far for hot model # example
    # if len(res.docs) != window_size:
    #     data_for_models = [
    #         [
    #             [0.53388956, -0.35487241, -1.45145732],
    #             [0.20356396, -0.5873226, -1.45145732],
    #             [0.21142885, -0.35487241, -1.33661515],
    #             [-0.35903823, -0.12242222, -1.1069308],
    #             [-0.5991797, -0.12242222, -0.87724645],
    #             [-1.04538143, -0.12242222, -0.64756211],
    #         ]
    #     ]
    #     val_1 = hot_models["lstm1"][1].predict(data_for_models)

    #     return ["single hot",f"{val_1}"]

    # hot goes here
    if len(res.docs) >= window_size:
        append_data = []
        for index in range(window_size):
            response_json = json.loads(res.docs[index].json)
            data = [
                response_json["humidity"],
                response_json["wind_v"],
                response_json["temp"],
            ]
            append_data.append(data)

        append_data.reverse()
        append_data = numpy.array(
            append_data
        )  # sth is wrong here!!! and locally it works:(
        data_for_models = append_data.reshape(1, window_size, features_size)

        val_1 = hot_models["lstm1"][1].predict(data_for_models)
        val_2 = hot_models["lstm2"][1].predict(data_for_models)
        val_3 = hot_models["lstm3"][1].predict(data_for_models)

        return ["multi hot",f"{val_1}", f"{val_2}", f"{val_3}"]

    return res
