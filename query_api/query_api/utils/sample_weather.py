from geopandas import GeoDataFrame
import fsspec

df = GeoDataFrame.from_file("query_api/resources/countries.geojson")

def sample_weather(country:str):
    #generate sample points
    return generate_sample_points(country)

    #query api for each point

#path for file is resources/countries.geojson for uvicorn
def generate_sample_points(country: str):
    
    return 'ok'