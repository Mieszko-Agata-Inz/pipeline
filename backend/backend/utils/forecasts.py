import datetime
import json
import logging
import redis
import numpy
from redis.commands.search.query import NumericFilter, Query
from backend.utils.cleanup import clean_aggregated
from backend.utils.conn import redisCli
import pygeohash as pgh


def get_forecast(
    lat,
    lon,
    coldstart_models,
    hot_models,
    mean_and_std,
    coldstart_models_biases,
):
    # window_size and features_size: depends on hot model
    window_size = 12
    features_size = 3

    geohash = "g".join([str(item) for item in pgh.encode(lat, lon, precision=3)])
    print(geohash)
    q = (
        Query(f"@geohash:{geohash}")
        .add_filter(
            NumericFilter(
                "timestamp",
                datetime.datetime.timestamp(datetime.datetime.utcnow()) - 21700,
                datetime.datetime.timestamp(datetime.datetime.utcnow()),
            )
        )
        .sort_by("timestamp", asc=False)
    )
    # log1 = logging.getLogger("uvicorn.info")
    # log1.info("%s", "redis call", exc_info=1)
    res = redisCli.ft("aggregated").search(q)

    # log1.info("%s", "redis close", exc_info=1)

    if len(res.docs) <= window_size:  # change required number of dataframes here
        # cold goes here
        q2 = (
            Query(f"@geohash:{geohash}")
            .paging(0, 1)
            .add_filter(
                NumericFilter(
                    "timestamp",
                    datetime.datetime.timestamp(datetime.datetime.utcnow()) - 60,
                    datetime.datetime.timestamp(datetime.datetime.utcnow()),
                )
            )
            .sort_by("timestamp", asc=False)
        )
        res = redisCli.ft("raw").search(q2)
        if len(res.docs) != 0:
            # return "no data"
            response_json = json.loads(res.docs[0].json)
            fdate = datetime.datetime.fromtimestamp(response_json["timestamp"])
            #### data for new pickles
            data = [
                lon,
                lat,
                response_json["temp"],
                response_json["humidity"],
                response_json["wind_v"],
                fdate.day,
                fdate.month,
                fdate.year,
                fdate.hour,
            ]

            print(data)

            #### data for old pickles
            # data = [
            #     response_json["humidity"],
            #     response_json["temp"],
            #     fdate.day,
            #     fdate.month,
            #     fdate.year,
            #     fdate.hour,
            # ]

            ################################
            val_1 = coldstart_models["xgb_1"][1].predict([data])
            val_2 = coldstart_models["xgb_2"][1].predict([data])
            val_3 = coldstart_models["xgb_3"][1].predict([data])
            # df_with_biases = coldstart_models_biases["xgb_1"][1]
            # val_1 = val_1 + df_with_biases[df_with_biases.index == 0]
            # val_2 = val_2 + df_with_biases[df_with_biases.index == 1]
            # val_3 = val_3 + df_with_biases[df_with_biases.index == 2]

            return {
                "hour0": (numpy.array([data[3], data[4], data[2]])).tolist(),
                "hour1": (
                    numpy.array(
                        [
                            val_1[0][3]
                            + coldstart_models_biases["xgb_1"][1]["humid"][0],
                            val_1[0][4]
                            + coldstart_models_biases["xgb_1"][1]["wind"][0],
                            val_1[0][2]
                            + coldstart_models_biases["xgb_1"][1]["temp"][0],
                        ]
                    )
                ).tolist(),  # ((val_1[2:5])).tolist(),
                "hour2": (
                    numpy.array(
                        [
                            val_2[0][3]
                            + coldstart_models_biases["xgb_2"][1]["humid"][1],
                            val_2[0][4]
                            + coldstart_models_biases["xgb_2"][1]["wind"][1],
                            val_2[0][2]
                            + coldstart_models_biases["xgb_2"][1]["temp"][1],
                        ]
                    )
                ).tolist(),
                "hour3": (
                    numpy.array(
                        [
                            val_3[0][3]
                            + coldstart_models_biases["xgb_3"][1]["humid"][2],
                            val_3[0][4]
                            + coldstart_models_biases["xgb_3"][1]["wind"][2],
                            val_3[0][2]
                            + coldstart_models_biases["xgb_3"][1]["temp"][2],
                        ]
                    )
                ).tolist(),
            }
            # return {
            #     "hour0": data[2:5],
            #     "hour1": (val_1[0][2:5]).tolist(),  # ((val_1[2:5])).tolist(),
            #     "hour2": (val_2[0][2:5]).tolist(),
            #     "hour3": (val_3[0][2:5]).tolist(),
            # }
        else:
            return "no data"
    #     # so far for hot model # example
    #     data_for_models = [
    #         [
    #             [89, 10, -5],
    #             [86, 8, -3],
    #             [84, 11, -3],
    #             [82, 12, -4],
    #             [83, 10, -5],
    #             [79, 10, -5],
    #         ]
    #     ]

    #     # mean and std for normalization
    #     training_mean = mean_and_std["data"][1]["mean"].values
    #     training_std = mean_and_std["data"][1]["std"].values

    #     # data normalization
    #     data = [
    #         (
    #             (numpy.array(data_for_models[0]) - training_mean) / training_std
    #         ).tolist()
    #     ]
    #     print(data)
    #     val_1 = hot_models["lstm_1"][1].predict(data)
    #     val_2 = hot_models["lstm_2"][1].predict(data)
    #     val_3 = hot_models["lstm_3"][1].predict(data)

    #     # data denormalization
    #     val_1 = (val_1[0] * training_std) + training_mean
    #     val_2 = (val_2[0] * training_std) + training_mean
    #     val_3 = (val_3[0] * training_std) + training_mean

    #     # # return values
    #     print(hot_models_biases["lstm_1"][1].values[0])
    # print(hot_models_biases["lstm_2"][1].values[0])
    # print(hot_models_biases["lstm_3"][1].values[0])

    # # return values
    # return {
    #     "hour0": ((data_for_models[0][5])).tolist(),
    #     "hour1": ((val_1 + hot_models_biases["lstm_1"][1].values[0])).tolist(),
    #     "hour2": ((val_2 + hot_models_biases["lstm_2"][1].values[0])).tolist(),
    #     "hour3": ((val_3 + hot_models_biases["lstm_3"][1].values[0],)).tolist(),
    # }

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
        append_data = numpy.array(append_data)
        data_for_models = append_data.reshape(1, window_size, features_size)

        # mean and std for normalization
        training_mean = mean_and_std["data"][1]["mean"].values
        training_std = mean_and_std["data"][1]["std"].values

        # data normalization
        data = [
            ((numpy.array(data_for_models[0]) - training_mean) / training_std).tolist()
        ]

        val_1 = hot_models["lstm_1"][1].predict(data)
        val_2 = hot_models["lstm_2"][1].predict(data)
        val_3 = hot_models["lstm_3"][1].predict(data)

        # data denormalization
        val_1 = (val_1[0] * training_std) + training_mean
        val_2 = (val_2[0] * training_std) + training_mean
        val_3 = (val_3[0] * training_std) + training_mean

        # print(hot_models_biases["lstm_1"][1].values[0])
        # print(hot_models_biases["lstm_2"][1].values[0])
        # print(hot_models_biases["lstm_3"][1].values[0])

        # return values
        return {
            "hour0": ((data_for_models[0][5])).tolist(),
            "hour1": ((val_1)).tolist(),
            "hour2": ((val_2)).tolist(),
            "hour3": ((val_3)).tolist(),
        }

    return "no data"
