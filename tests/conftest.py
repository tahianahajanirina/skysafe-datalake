import shutil
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    if not shutil.which("java"):
        pytest.skip("Java not found — Spark tests require a JVM (run inside Docker).")

    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("sky_safe_tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark session failed to start: {exc}")

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
