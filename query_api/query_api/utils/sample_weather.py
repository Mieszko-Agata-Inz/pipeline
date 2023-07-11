from datapackage import Package

package = Package('https://datahub.io/core/geo-countries/datapackage.json')

def sample_weather(country:str):
    #generate sample points
    return generate_sample_points(country)

    #query api for each point


def generate_sample_points(country: str):
    return package.resource_names