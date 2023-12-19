import datetime
import json
import redis
from backend.utils.conn import redisCli
from redis.commands.search.query import NumericFilter, Query


def clean_raw(timestamp):
    q = (
        Query("*")
        .paging(0, 30)
        .add_filter(
            NumericFilter(
                "timestamp",
                0,
                datetime.datetime.timestamp(datetime.datetime.utcnow())
                - 60,  # last 1 minute remains
            )
        )
    )
    res = redisCli.ft("raw").search(q)
    for doc in res.__dict__["docs"]:
        redisCli.json().delete(doc["id"])


def clean_aggregated():
    q = Query("*").add_filter(
        NumericFilter(
            "timestamp",
            0,
            datetime.datetime.timestamp(datetime.datetime.utcnow())
            - 21700,  # last 6 hours remain
        )
    )
    res = redisCli.ft("aggregated").search(q)
    for doc in res.__dict__["docs"]:
        redisCli.json().delete(doc["id"])
