import os
from fastapi import FastAPI
from dotenv import load_dotenv

from query_api.utils.sample_weather import sample_weather
load_dotenv()

app = FastAPI()


@app.get("/")
async def root():
    return {"message": f"{os.getenv('WEATHER_API_KEY')}"}

@app.get("/weather/{country}")
async def weather(country: str):
    result = sample_weather(country)
    return {"message": result}