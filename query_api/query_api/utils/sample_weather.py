from geopandas import GeoDataFrame
import fsspec

def sample_weather(country:str):
    #generate sample points
    return generate_sample_points(country)

    #query api for each point

#path for file is resources/countries.geojson for uvicorn
def generate_sample_points(country: str):
    df = GeoDataFrame.from_file("query_api/resources/countries.geojson")
    return 'ok'