import pytest
import extract_weather 
import sys
import os
import fixture
import src


sys.path.insert(0,)

@fixture
def send_weather_data():
    weather_data=[{
        "temperature_2":45,
        "relative_humidity_2m":99.2,
        "wind_speed_10m":1,
        "wind_direction_10m":2,
        "wind_gusts_10m":3,
        "precipitation":3,
        "rain":3,
        "cloud_cover":3,
        "weather_code":3,
        "visibility":3,
        "location":
            [
                {"latitude": 48.709632, "longitude": 2.208563},   
                {"latitude": 43.629421, "longitude": 1.367789}
            ] 
        }
    ]
