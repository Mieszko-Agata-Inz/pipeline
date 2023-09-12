import requests
import os
import time
import json
import asyncio
import pygeohash as pgh

from confluent_kafka.cimpl import Producer

api_url = "https://api.openweathermap.org/data/2.5/weather?"
p = Producer({'bootstrap.servers': os.getenv('KAFKA_BROKER')})

#locations - list with geohashes
async  def actual_weather_async(geohashes: list, func_get, func_set):
    # i parameter is temporary if stop request didn't respond - for Poland max 10 minutes
    i=0
    while (i < 20):
        i+=1
        for geohash in geohashes:
            await asyncio.sleep(1)
            if (func_get() == False):
                func_set()
                return

            location = pgh.decode(geohash)

            response = requests.get(api_url + "lat=" + str(location[0]) + "&lon=" + str(location[1]) +"&appid=" + os.getenv('WEATHER_API_KEY'))
            response_json = response.json()
            latitude = str(location[0])
            longtitude = str(location[1])
            hash = latitude + longtitude

            #for json structure:  geohash, lat, long, temperature in Celsius degree, wind velocity, humidity, count - used for aggregations
            #from API https://openweathermap.org/api/one-call-3
            temperature = response_json["main"]["temp"] - 273.15
            wind_v = response_json["main"]["wind_speed"]
            humidity = response_json["main"]["humidity"]

            data = json.dumps({"hash":geohash, "lat":location[0] , "long":location[1] , "temp":temperature, "wind_v":wind_v, "humidity":humidity,  "count":1})
            p.produce('weather_data', data)
            p.produce('raw_weather_data', data)
            p.flush()
            yield data