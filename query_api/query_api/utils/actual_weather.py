import requests
import os
import time

api_url = "https://api.openweathermap.org/data/2.5/weather?"

def actual_weather(locations: list):
    #get current weather
    return current_weather_one_location(locations)

    #query api for each point

def current_weather_one_location(locations: list):
    #returns current weather in given locations
    response = []
    for location in locations:
        time.sleep(1)
        response.append(requests.get(api_url + "lat=" + str(location[0]) + "&lon=" + str(location[1]) +"&appid=" + os.getenv('WEATHER_API_KEY')))
   
    return response