"""Tests for serverless_function_call.py constants."""
from serverless_function_call import FRANCE_BOUNDING_BOX, LAMBDA_URL


class TestServerlessConfig:
    def test_bounding_box_is_list_of_four(self):
        assert isinstance(FRANCE_BOUNDING_BOX, list)
        assert len(FRANCE_BOUNDING_BOX) == 4

    def test_bounding_box_covers_france(self):
        lat_min, lat_max, lon_min, lon_max = FRANCE_BOUNDING_BOX
        assert lat_min < 42  # South of France
        assert lat_max > 51  # North of France
        assert lon_min < -5  # West of France
        assert lon_max > 9   # East of France

    def test_lambda_url_is_https(self):
        assert LAMBDA_URL.startswith("https://")
