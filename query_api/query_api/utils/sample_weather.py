from geopandas import GeoDataFrame
from polygeohasher import polygeohasher
# import pygeohash as pgh
import fsspec


#path for file is resources/countries.geojson for uvicorn - if not: query_api/resources/countries.geojson
df = GeoDataFrame.from_file("query_api/resources/countries.geojson")

def sample_weather(country:str):
    #generate sample points
    return generate_sample_points(country)

    #query api for each point

def generate_sample_points(country: str):

    specified_country_df = df.loc[df['NAME_EN']==country]

    #dataframe with geohashes
    #geohash_level == 3 it means points distance equals around 156 km
    geohashed_df = polygeohasher.create_geohash_list(specified_country_df, 3 , inner = False)
    geohashed_df_optimized = polygeohasher.geohash_optimizer(geohashed_df, 3,3,3)
    temp = list(geohashed_df_optimized['optimized_geohash_list'].values)
    geohash_list = []
    [geohash_list.append(tuple(x)) for x in temp if tuple(x) not in geohash_list]
    geohash_list = list(set(geohash_list))
    # number_of_geohash = len(geohash_list)
    # geohashes = []
    # for index in range(0, len(geohash_list)):
    #     geohashes.append(pgh.decode(geohash_list[index]))
    return geohash_list, number_of_geohash
