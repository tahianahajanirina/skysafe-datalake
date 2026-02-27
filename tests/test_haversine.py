"""Tests for the Haversine Spark expression in combine_spark.py."""
import pytest
from pyspark.sql import functions as F
from combine_spark import haversine_expr


def _distance(spark, lat1, lon1, lat2, lon2) -> float:
    df = spark.createDataFrame(
        [{"lat1": lat1, "lon1": lon1, "lat2": lat2, "lon2": lon2}]
    )
    return df.select(
        haversine_expr(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")).alias("d")
    ).first()["d"]


class TestHaversine:
    def test_same_point_is_zero(self, spark):
        assert _distance(spark, 48.85, 2.35, 48.85, 2.35) == pytest.approx(0.0, abs=0.01)

    def test_paris_to_lyon(self, spark):
        # ~392 km
        d = _distance(spark, 48.8566, 2.3522, 45.7640, 4.8357)
        assert 380 < d < 410

    def test_paris_to_toulouse(self, spark):
        # ~589 km
        d = _distance(spark, 48.8566, 2.3522, 43.6047, 1.4442)
        assert 575 < d < 605

    def test_symmetry(self, spark):
        d1 = _distance(spark, 48.8566, 2.3522, 43.6047, 1.4442)
        d2 = _distance(spark, 43.6047, 1.4442, 48.8566, 2.3522)
        assert d1 == pytest.approx(d2, rel=1e-6)

    def test_distance_is_positive(self, spark):
        assert _distance(spark, 0.0, 0.0, 1.0, 1.0) > 0
