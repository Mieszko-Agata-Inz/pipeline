import redis

redisCli = redis.Redis(
    host="redis",
    port=6379,
    charset="utf-8",
    decode_responses=True,
    password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81",
    )