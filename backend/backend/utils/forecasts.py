import datetime
import json
import logging
import redis
import numpy
from redis.commands.search.query import NumericFilter, Query
from backend.utils.cleanup import clean_aggregated
from backend.utils.conn import redisCli
import pygeohash as pgh
import numpy as np


def get_forecast(
    lat,
    lon,
    coldstart_models,
    hot_models,
    mean_and_std,
    hot_models_biases,
    coldstart_models_biases,
):
    # window_size and features_size: depends on hot model
    window_size = 6
    features_size = 3
    geohash = pgh.encode(lat, lon, precision=3)
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

    if len(res.docs) == 0:  # change required number of dataframes here
        # cold goes here
        ######## data from kafka ########
        # q2 = (
        #     Query(f"@geohash:{geohash}")
        #     .paging(0, 1)
        #     .add_filter(
        #         NumericFilter(
        #             "timestamp",
        #             datetime.datetime.timestamp(datetime.datetime.utcnow()) - 10,
        #             datetime.datetime.timestamp(datetime.datetime.utcnow()),
        #         )
        #     )
        #     .sort_by("timestamp", asc=False)
        # )
        # res = redisCli.ft("raw").search(q)
        # if len(res.docs) == 0:
        #     return "no data"
        # response_json = json.loads(res.docs[0].json)
        # fdate = datetime.datetime.fromtimestamp(response_json["timestamp"])
        # data = [
        #     lon,
        #     lat,
        #     response_json["temp"],
        #     response_json["humidity"],
        #     response_json["wind_v"],
        #     fdate.day,
        #     fdate.month,
        #     fdate.year,
        #     fdate.hour,
        # ]

        ################################

        # json.loads(res.docs[0].json)["temp"]
        dummy_data = [93.03, 6.0, 1, 1, 2015, 4]  # relh  sknt  day  month  year  hour
        # data if new models updated
        # dummy_data_new = [22,50,-3,93.03, 18.0, 1, 1, 2015, 4]  # lon lat tmpc relh  speed(km/h)  day  month  year  hour
        val_1 = coldstart_models["xgb_1"][1].predict([dummy_data])
        val_2 = coldstart_models["xgb_2"][1].predict([dummy_data])
        val_3 = coldstart_models["xgb_3"][1].predict([dummy_data])
        # biases will be here
        return {
            "hour0": dummy_data[2:5],
            "hour1": ((val_1[0:3])).tolist(),
            "hour2": ((val_2[0:3])).tolist(),
            "hour3": ((val_3[0:3])).tolist(),
        }
    # so far for hot model # example
    if len(res.docs) != window_size:
        data_for_models = [
            [
                [89, 10, -5],
                [86, 8, -3],
                [84, 11, -3],
                [82, 12, -4],
                [83, 10, -5],
                [79, 10, -5],
            ]
        ]

        # mean and std for normalization
        training_mean = mean_and_std["data"][1]["mean"].values
        training_std = mean_and_std["data"][1]["std"].values

        # data normalization
        data = [
            ((np.array(data_for_models[0]) - training_mean) / training_std).tolist()
        ]
        print(data)
        val_1 = hot_models["lstm_1"][1].predict(data)
        val_2 = hot_models["lstm_2"][1].predict(data)
        val_3 = hot_models["lstm_3"][1].predict(data)

        # data denormalization
        val_1 = (val_1[0] * training_std) + training_mean
        val_2 = (val_2[0] * training_std) + training_mean
        val_3 = (val_3[0] * training_std) + training_mean

        # return values
        return {
            "hour0": data_for_models[0][5],
            "hour1": ((val_1)).tolist(),
            "hour2": ((val_2)).tolist(),
            "hour3": ((val_3)).tolist(),
        }

    # hot goes here
    if len(res.docs) == window_size:
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
        append_data = numpy.array(append_data)
        data_for_models = append_data.reshape(1, window_size, features_size)

        # mean and std for normalization
        training_mean = mean_and_std["data"][1]["mean"].values
        training_std = mean_and_std["data"][1]["std"].values

        # data normalization
        data = [
            ((np.array(data_for_models[0]) - training_mean) / training_std).tolist()
        ]
        print(data)
        val_1 = hot_models["lstm_1"][1].predict(data)
        val_2 = hot_models["lstm_2"][1].predict(data)
        val_3 = hot_models["lstm_3"][1].predict(data)

        # data denormalization
        val_1 = (val_1[0] * training_std) + training_mean
        val_2 = (val_2[0] * training_std) + training_mean
        val_3 = (val_3[0] * training_std) + training_mean

        # return values
        return {
            "hour0": data_for_models[0][5],
            "hour1": ((val_1)).tolist(),
            "hour2": ((val_2)).tolist(),
            "hour3": ((val_3)).tolist(),
        }

    return res
