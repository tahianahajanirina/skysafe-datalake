"""Tests for the risk scoring and categorisation logic reproduced using a local Spark session."""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def _build_row(spark, **kwargs):
    """Helper: create a single-row DataFrame with defaults for every risk-score column."""
    defaults = {
        "w_weather_code": 0,
        "w_wind_gusts_10m": 0.0,
        "w_precipitation": 0.0,
        "w_visibility": 10000.0,
        "w_cloud_cover": 0,
        "on_ground": False,
        "baro_altitude": 10000.0,
    }
    defaults.update(kwargs)
    return spark.createDataFrame([defaults])


def _score(spark, **kwargs):
    """Return the computed risk_score integer for a given set of conditions."""
    df = _build_row(spark, **kwargs)

    risk_expr = (
        F.when(F.col("w_weather_code") >= 95, F.lit(40)).otherwise(F.lit(0))
        + F.when(F.col("w_wind_gusts_10m") > 80, F.lit(25))
           .when(F.col("w_wind_gusts_10m") > 50, F.lit(10))
           .otherwise(F.lit(0))
        + F.when(F.col("w_precipitation") > 5, F.lit(20))
           .when(F.col("w_precipitation") > 0, F.lit(10))
           .otherwise(F.lit(0))
        + F.when(F.col("w_visibility") < 1000, F.lit(20))
           .when(F.col("w_visibility") < 3000, F.lit(10))
           .otherwise(F.lit(0))
        + F.when(F.col("w_cloud_cover") > 80, F.lit(10))
           .when(F.col("w_cloud_cover") > 50, F.lit(5))
           .otherwise(F.lit(0))
        + F.when(
            (F.col("on_ground") == False) & (F.col("baro_altitude") < 300),  # noqa: E712
            F.lit(15),
          ).otherwise(F.lit(0))
    )

    return df.withColumn("risk_score", risk_expr.cast(IntegerType())).first()["risk_score"]


def _category(score: int) -> str:
    if score >= 60:
        return "HIGH"
    if score >= 30:
        return "MEDIUM"
    return "LOW"


class TestRiskScore:
    def test_clear_sky_scores_zero(self, spark):
        assert _score(spark) == 0

    def test_thunderstorm_adds_40(self, spark):
        assert _score(spark, w_weather_code=95) == 40

    def test_strong_gusts_adds_25(self, spark):
        assert _score(spark, w_wind_gusts_10m=85.0) == 25

    def test_moderate_gusts_adds_10(self, spark):
        assert _score(spark, w_wind_gusts_10m=60.0) == 10

    def test_heavy_precipitation_adds_20(self, spark):
        assert _score(spark, w_precipitation=6.0) == 20

    def test_light_precipitation_adds_10(self, spark):
        assert _score(spark, w_precipitation=1.0) == 10

    def test_low_visibility_adds_20(self, spark):
        assert _score(spark, w_visibility=500.0) == 20

    def test_moderate_visibility_adds_10(self, spark):
        assert _score(spark, w_visibility=2000.0) == 10

    def test_high_cloud_cover_adds_10(self, spark):
        assert _score(spark, w_cloud_cover=90) == 10

    def test_medium_cloud_cover_adds_5(self, spark):
        assert _score(spark, w_cloud_cover=60) == 5

    def test_low_altitude_in_flight_adds_15(self, spark):
        assert _score(spark, on_ground=False, baro_altitude=200.0) == 15

    def test_low_altitude_on_ground_adds_nothing(self, spark):
        assert _score(spark, on_ground=True, baro_altitude=100.0) == 0

    def test_worst_case_capped_accumulation(self, spark):
        # thunderstorm + strong gusts + heavy rain + near-zero visibility + full clouds + low altitude
        s = _score(
            spark,
            w_weather_code=99,
            w_wind_gusts_10m=90.0,
            w_precipitation=10.0,
            w_visibility=100.0,
            w_cloud_cover=100,
            on_ground=False,
            baro_altitude=150.0,
        )
        assert s == 40 + 25 + 20 + 20 + 10 + 15  # == 130


class TestRiskCategory:
    def test_low(self):
        assert _category(0) == "LOW"
        assert _category(29) == "LOW"

    def test_medium(self):
        assert _category(30) == "MEDIUM"
        assert _category(59) == "MEDIUM"

    def test_high(self):
        assert _category(60) == "HIGH"
        assert _category(130) == "HIGH"
