from pyspark.sql import functions as F

from helpers import get_spark, latest_partition, output_path, join_path, read_json, logger


def format_weather_main(spark=None) -> str:
    """
    Lit weather_raw.json (data/raw/open_meteo/weather/date=.../hour=.../)
    et crée un Parquet nettoyé dans data/formatted/open_meteo/weather/.
    """
    spark = spark or get_spark()

    # Lecture des fichier JSON depuis la dernière partition
    partition_dir = latest_partition("raw", "open_meteo", "weather")
    filepath = join_path(partition_dir, "weather_raw.json")
    logger.info("Lecture du fichier brut : %s", filepath)
    raw_points = read_json(filepath)  # liste de dicts (un par point géographique)

    records = []
    for point in raw_points:
        current = point.get("current", {})
        record = {
            "latitude":               point.get("latitude"),
            "longitude":              point.get("longitude"),
            "elevation":              point.get("elevation"),
            "weather_time":           current.get("time"),
            "temperature_2m":         current.get("temperature_2m"),
            "relative_humidity_2m":   current.get("relative_humidity_2m"),
            "wind_speed_10m":         current.get("wind_speed_10m"),
            "wind_direction_10m":     current.get("wind_direction_10m"),
            "wind_gusts_10m":         current.get("wind_gusts_10m"),
            "precipitation":          current.get("precipitation"),
            "rain":                   current.get("rain"),
            "cloud_cover":            current.get("cloud_cover"),
            "weather_code":           current.get("weather_code"),
            "visibility":             current.get("visibility"),
            "extracted_at":           point.get("_extracted_at"),
        }
        records.append(record)

    logger.info("%d points météo transformés en records", len(records))

    df = spark.createDataFrame(records)

    # Normalisation des dates en UTC
    # weather_time arrive en string ISO local (ex: "2026-02-26T14:00") → timestamp UTC
    # extracted_at arrive en string ISO UTC → timestamp UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df = (
        df
        .withColumn("weather_time", F.to_utc_timestamp(F.to_timestamp("weather_time"), "UTC"))
        .withColumn("extracted_at", F.to_utc_timestamp(F.to_timestamp("extracted_at"), "UTC"))
    )

    dest = output_path("formatted", "open_meteo", "weather")
    df.write.mode("overwrite").parquet(dest)
    logger.info("Parquet écrit dans : %s", dest)

    return dest
