import requests
import os
import time
import json
import asyncio

from confluent_kafka.cimpl import Producer

api_url = "https://api.openweathermap.org/data/2.5/weather?"
p = Producer({'bootstrap.servers': os.getenv('KAFKA_BROKER')})

async  def actual_weather_async(locations: list, func_get, func_set):
    # i parameter is temporary if stop request didn't respond - for Poland max 10 minutes
    i=0
    while (i < 20):
        i+=1
        for location in locations:
            await asyncio.sleep(1)
            if (func_get() == False):
                func_set()
                return
            response = requests.get(api_url + "lat=" + str(location[0]) + "&lon=" + str(location[1]) +"&appid=" + os.getenv('WEATHER_API_KEY'))
            response_json = response.json()
            latitude = str(location[0])
            longtitude = str(location[1])
            hash = latitude + longtitude
            temperature = response_json["main"]["temp"] - 273.15
            data = json.dumps({"hash":hash, "temp":temperature})
            p.produce('weather_data', data)
            p.flush()
            yield data