import requests
import os
import time
import json
import asyncio
import pygeohash as pgh
import datetime

from confluent_kafka.cimpl import Producer

api_url = "https://api.openweathermap.org/data/2.5/weather?"
p = Producer({"bootstrap.servers": os.getenv("KAFKA_BROKER")})


async def actual_weather_async(geohashes, func_get, func_set):
    # i parameter is temporary if stop request didn't respond - for Poland max 10 minutes
    i = 0
    locations = []
    for index in range(0, len(geohashes)):
        locations.append(pgh.decode(geohashes[index]))
    locations_list = list(locations)
    fails=0
    ts = []
    while i < 2:
        i += 1
        geo_index = 0
        for location in locations_list:
            await asyncio.sleep(1)
            if func_get() == False:
                func_set()
                return
            response = requests.get(
                api_url
                + "lat="
                + str(location[0])
                + "&lon="
                + str(location[1])
                + "&appid="
                + os.getenv("WEATHER_API_KEY")
            )
            if response.status_code != 200:
                fails = fails + 1
            response_json = response.json()
            latitude = str(location[0])
            longtitude = str(location[1])
            hash = latitude + longtitude
            # for json structure:  geohash, timestamp,  lat, long, temperature in Celsius degree, wind velocity, humidity, count - used for aggregations
            # from API https://openweathermap.org/api/one-call-3
            temperature = response_json["main"]["temp"] - 273.15
            wind_v = response_json["wind"]["speed"]
            humidity = response_json["main"]["humidity"]
            geohash = "g".join([str(item) for item in geohashes[geo_index]])

            data = json.dumps(
                {
                    "timestamp": str(
                        datetime.datetime.timestamp(datetime.datetime.utcnow())
                    ).split(".")[0],
                    "lat": location[0],
                    "long": location[1],
                    "temp": temperature,
                    "wind_v": wind_v,
                    "humidity": humidity,
                    "count": 1,
                }
            )

            data_raw = json.dumps(
                {
                    "timestamp": str(
                        datetime.datetime.timestamp(datetime.datetime.utcnow())
                    ).split(".")[0],
                    "lat": location[0],
                    "long": location[1],
                    "temp": temperature,
                    "wind_v": wind_v,
                    "humidity": humidity,
                }
            )
            ts.append(( str(datetime.datetime.timestamp(datetime.datetime.utcnow()))[:10], response_json["dt"]))

            geo_index += 1
            p.produce("weather_data", key=geohash, value=data)
            p.produce("raw_weather_data", key=geohash, value=data_raw)
            p.flush()
    print(f"fails: {fails}")
    print(ts)

