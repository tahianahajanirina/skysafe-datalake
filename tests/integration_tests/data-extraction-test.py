import pytest
import sys
from src.extract_weather import fetch_weather
import src  

def test_weather_api_response():
    fetch_weather()=="data/raw/open_meteo/weather/weather_raw.json"

