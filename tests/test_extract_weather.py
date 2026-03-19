"""Tests for extract_weather.py configuration and constants."""
from extract_weather import DEFAULT_WEATHER_POINTS, WEATHER_VARIABLES


class TestWeatherConfig:
    def test_default_points_count(self):
        """Should have 6 French weather stations."""
        assert len(DEFAULT_WEATHER_POINTS) == 6

    def test_each_point_has_lat_lon(self):
        for point in DEFAULT_WEATHER_POINTS:
            assert "latitude" in point
            assert "longitude" in point
            assert isinstance(point["latitude"], float)
            assert isinstance(point["longitude"], float)

    def test_points_are_in_france(self):
        """All points should be roughly within France's bounding box."""
        for point in DEFAULT_WEATHER_POINTS:
            assert 41.0 < point["latitude"] < 52.0, f"Latitude out of France: {point}"
            assert -6.0 < point["longitude"] < 10.0, f"Longitude out of France: {point}"

    def test_weather_variables_contains_required_fields(self):
        for field in ("temperature_2m", "wind_speed_10m", "precipitation", "visibility"):
            assert field in WEATHER_VARIABLES
