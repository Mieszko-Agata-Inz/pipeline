import asyncio
import os
from fastapi import FastAPI, Depends
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient
from time import sleep


# Importing utility functions from query_api module
from query_api.utils.sample_weather import sample_weather
from query_api.utils.actual_weather import actual_weather_async

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
)
# Create an asyncio lock for synchronizing access to shared resources
lock = asyncio.Lock()

# Shared flag to control access to the get_weather request
isOpen = True
# Shared flag to indicate if the service is currently working
isWork = False


# async def on_startup():
#     geohashes, number = sample_weather("Poland")
#     try:
#         # Make an asynchronous call to fetch actual weather data
#         await actual_weather_async(geohashes, get_shared_data, set_shared_data_true)
#     except:
#         print("issue querying api, reconnecting")


# app.add_event_handler("startup", on_startup)


# Function to get the current state of 'isOpen'
def get_shared_data():
    return isOpen


# Function to set 'isOpen' to True
def set_shared_data_true():
    global isOpen
    isOpen = True


# Function to set 'isOpen' to False
def set_shared_data_false():
    global isOpen
    isOpen = False


# Function to set 'isWork' to False, indicating inactivity
def set_inactive():
    global isWork
    isWork = False


# Function to set 'isWork' to False, should be True to indicate activity
def set_active():
    global isWork
    isWork = False  # This seems to be a mistake; it should likely be set to True


# Function to get the current state of 'isWork'
def get_active():
    global isWork
    return isWork


# Root endpoint, returns the weather API key from environment variables
@app.get("/")
async def root():
    return {"message": f"{os.getenv('WEATHER_API_KEY')}"}


# Endpoint to get weather data for a given country
@app.get("/weather/{country}")
async def weather(country: str):
    global lock
    print("start")
    # Acquire lock to ensure exclusive access to shared resources
    async with lock:
        # If already active, return immediately
        if get_active():
            return
        # Set active status
        set_active()
    while True:
        # Acquire lock again within the loop for shared resource access
        async with lock:
            # Check active status again within the loop
            if get_active():
                return
        # Get geohashes and number of locations for the specified country
        geohashes, number = sample_weather(country)
        try:
            # Make an asynchronous call to fetch actual weather data
            await actual_weather_async(geohashes, get_shared_data, set_shared_data_true)
        except:
            print("issue querying api, reconnecting")
    return 200


# Endpoint to stop the weather data fetching process
@app.get("/stop")
async def stopdata():
    global lock
    # Acquire lock to modify shared data
    async with lock:
        # Set shared data flags to indicate stopping the process
        set_shared_data_false()
        set_inactive()


# Function to simulate a request to /weather/{country} after startup
# def simulate_request_after_startup():
#     # Wait for FastAPI to fully start (adjust as needed)
#     sleep(5)

#     # Use TestClient to simulate a request
#     client = TestClient(app)
#     print("Client starts")
#     response = client.get("/weather/Poland")
#     print(response.status_code)
#     print(response.json())


# if __name__ == "__main__":
#     # Run the simulation after the application starts
#     simulate_request_after_startup()
