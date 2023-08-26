import os
from fastapi import FastAPI, Depends
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse

#remove query_api to run with uvicorn - if not: query_api.utils.sample_weather
from query_api.utils.sample_weather import sample_weather
from query_api.utils.actual_weather import actual_weather_async

load_dotenv()

app = FastAPI()

#isOpen for get_weather request
isOpen = True
def get_shared_data():
    return isOpen
def set_shared_data_true():
    global isOpen
    isOpen = True

def set_shared_data_false():
    global isOpen
    isOpen = False

@app.get("/")
async def root():
    return {"message": f"{os.getenv('WEATHER_API_KEY')}"}

@app.get("/weather/{country}")
async def weather(country: str):
    locations, number = sample_weather(country)
    return StreamingResponse(actual_weather_async(locations, get_shared_data,set_shared_data_true))

@app.get("/stop")
async def stopdata():
    set_shared_data_false()

