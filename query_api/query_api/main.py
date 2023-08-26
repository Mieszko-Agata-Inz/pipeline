import json
import os
from fastapi import FastAPI
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.cimpl import NewTopic, Producer

#remove query_api to run with uvicorn - if not: query_api.utils.sample_weather
from query_api.utils.sample_weather import sample_weather
# from query_api.utils.actual_weather import actual_weather

#new branch
import asyncio
import time
import requests
from fastapi.responses import StreamingResponse

api_url = "https://api.openweathermap.org/data/2.5/weather?"

load_dotenv()

p = Producer({'bootstrap.servers': os.getenv('KAFKA_BROKER')})

app = FastAPI()

isOpen = True

async  def actual_weather_async(locations: list):
    i=0
    global isOpen
    while (i < 2):
        i+=1
        for location in locations:
            await asyncio.sleep(1)
            if (isOpen == False):
                isOpen = True
                return
            response = requests.get(api_url + "lat=" + str(location[0]) + "&lon=" + str(location[1]) +"&appid=" + os.getenv('WEATHER_API_KEY'))
            response_json = response.json()
            latitude = location[0]
            longtitude = location[1]
            temperature = response_json["main"]["temp"] - 273.15
            data = json.dumps({"lat":latitude, "lon":longtitude, "temp":temperature})
            p.produce('weather_data', data)
            p.flush()
            yield data

@app.get("/")
async def root():
    return {"message": f"{os.getenv('WEATHER_API_KEY')}"}

@app.get("/weather/{country}")
async def weather(country: str):
    locations, number = sample_weather(country)
    return StreamingResponse(actual_weather_async(locations))

@app.get("/stop")
async def stopdata():
    global isOpen
    isOpen = False

