import asyncio
import os
from fastapi import FastAPI, Depends
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse

#remove query_api to run with uvicorn - if not: query_api.utils.sample_weather
from query_api.utils.sample_weather import sample_weather
from query_api.utils.actual_weather import actual_weather_async

load_dotenv()

app = FastAPI()

lock = asyncio.Lock()

#isOpen for get_weather request
isOpen = True
isWork = False
def get_shared_data():
    return isOpen
def set_shared_data_true():
    global isOpen
    isOpen = True

def set_shared_data_false():
    global isOpen
    isOpen = False

def set_inactive():
    global isWork
    isWork = False

def set_active():
    global isWork
    isWork = False

def get_active():
    global isWork
    return isWork

@app.get("/")
async def root():
    return {"message": f"{os.getenv('WEATHER_API_KEY')}"}

@app.get("/weather/{country}")
async def weather(country: str):
    global lock
    print("start")
    async with lock:
        if get_active():
            return
        set_active()
    while True:
        async with lock:
            if get_active():
                return
        geohashes, number = sample_weather(country)
        try:
            await actual_weather_async(geohashes, get_shared_data,set_shared_data_true)
        except:
            print("issue while querying api, retry")
    return 200

@app.get("/stop")
async def stopdata():
    global lock
    async with lock:
        set_shared_data_false()
        set_inactive()

