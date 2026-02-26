"""
combine_spark.py
----------------
Job Spark : formatted → enriched
Jointure spatiale vols × météo + calcul du Score de Risque.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from helpers import get_spark, latest_partition, output_path, logger


# ─────────────────────────────────────────────────────────────────────────────
# Haversine via expressions Spark natives (pas de UDF Python)
# ─────────────────────────────────────────────────────────────────────────────

def haversine_expr(lat1, lon1, lat2, lon2):
    """
    Retourne une Column Spark = distance en km (formule de Haversine).
    Utilise uniquement des fonctions Spark natives → pas de sérialisation Python.
    """
    R = 6371.0  # rayon moyen de la Terre (km)
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    d_phi = F.radians(lat2 - lat1)
    d_lambda = F.radians(lon2 - lon1)
    a = (
        F.sin(d_phi / 2) ** 2
        + F.cos(phi1) * F.cos(phi2) * F.sin(d_lambda / 2) ** 2
    )
    return F.lit(R) * 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))


# ─────────────────────────────────────────────────────────────────────────────
# JOB COMBINE
# ─────────────────────────────────────────────────────────────────────────────

def combine_data_main(spark=None) -> str:
    """
    Jointure spatiale vols × météo et calcul du Score de Risque.

    Étapes :
      1. Lire les Parquet formatés (flights + weather) via latest_partition().
      2. Cross join vols × points météo, calculer la distance Haversine
         et ne garder que le point météo le plus proche de chaque vol.
      3. Calculer un score de risque (0–100).
      4. Ajouter risk_category : LOW / MEDIUM / HIGH.
      5. Écrire en Parquet dans data/enriched/sky_safe/flights_weather/.

    Returns : chemin du répertoire Parquet écrit.
    """
    logger.info("=== Démarrage combine_data ===")

    # ── 0. Spark ─────────────────────────────────────────────────────────────
    if spark is None:
        spark = get_spark()

    # ── 1. Lecture des Parquet formatés ──────────────────────────────────────
    flights_dir = latest_partition("formatted", "opensky", "flights")
    weather_dir = latest_partition("formatted", "open_meteo", "weather")

    logger.info("Lecture flights depuis : %s", flights_dir)
    logger.info("Lecture weather depuis : %s", weather_dir)

    df_flights = spark.read.parquet(flights_dir)
    df_weather = spark.read.parquet(weather_dir)

    logger.info("Vols chargés : %d | Points météo chargés : %d",
                df_flights.count(), df_weather.count())

    # ── 2. Préfixer les colonnes météo pour éviter les collisions ────────────
    weather_cols_to_prefix = [c for c in df_weather.columns if c not in ("latitude", "longitude")]
    for col_name in weather_cols_to_prefix:
        df_weather = df_weather.withColumnRenamed(col_name, "w_" + col_name)
    df_weather = (
        df_weather
        .withColumnRenamed("latitude", "w_latitude")
        .withColumnRenamed("longitude", "w_longitude")
    )

    # ── 3. Cross join + distance Haversine ───────────────────────────────────
    df_cross = df_flights.crossJoin(df_weather)

    df_cross = df_cross.withColumn(
        "dist_km",
        haversine_expr(
            F.col("latitude"), F.col("longitude"),
            F.col("w_latitude"), F.col("w_longitude"),
        ),
    )

    # ── 4. Garder uniquement le point météo le plus proche par vol ───────────
    window_nearest = Window.partitionBy("icao24").orderBy(F.col("dist_km").asc())

    df_nearest = (
        df_cross
        .withColumn("_rank", F.row_number().over(window_nearest))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    logger.info("Lignes après nearest-weather join : %d", df_nearest.count())

    # ── 5. Calcul du Score de Risque (0–100) ────────────────────────────────
    #   - Orage  (weather_code >= 95)      → +40
    #   - Rafales > 80 km/h               → +25  |  > 50 km/h → +10
    #   - Précipitations > 5 mm           → +20  |  > 0 mm    → +10
    #   - Visibilité < 1 000 m            → +20  |  < 3 000 m → +10
    #   - Couverture nuageuse > 80 %      → +10  |  > 50 %    → +5
    #   - Altitude baro < 300 m (en vol)  → +15

    risk_expr = (
        # Orage
        F.when(F.col("w_weather_code") >= 95, F.lit(40)).otherwise(F.lit(0))

        # Rafales
        + F.when(F.col("w_wind_gusts_10m") > 80, F.lit(25))
           .when(F.col("w_wind_gusts_10m") > 50, F.lit(10))
           .otherwise(F.lit(0))

        # Précipitations
        + F.when(F.col("w_precipitation") > 5, F.lit(20))
           .when(F.col("w_precipitation") > 0, F.lit(10))
           .otherwise(F.lit(0))

        # Visibilité
        + F.when(F.col("w_visibility") < 1000, F.lit(20))
           .when(F.col("w_visibility") < 3000, F.lit(10))
           .otherwise(F.lit(0))

        # Couverture nuageuse
        + F.when(F.col("w_cloud_cover") > 80, F.lit(10))
           .when(F.col("w_cloud_cover") > 50, F.lit(5))
           .otherwise(F.lit(0))

        # Altitude basse + en vol (on_ground == False)
        + F.when(
            (F.col("on_ground") == False) & (F.col("baro_altitude") < 300),  # noqa: E712
            F.lit(15),
          ).otherwise(F.lit(0))
    )

    df_scored = df_nearest.withColumn("risk_score", risk_expr.cast(IntegerType()))

    # ── 6. Catégorisation du risque ──────────────────────────────────────────
    df_scored = df_scored.withColumn(
        "risk_category",
        F.when(F.col("risk_score") >= 60, F.lit("HIGH"))
         .when(F.col("risk_score") >= 30, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW")),
    )

    # ── 7. Écriture Parquet (enriched) ───────────────────────────────────────
    out_dir = output_path("enriched", "sky_safe", "flights_weather")
    logger.info("Écriture Parquet enrichi vers : %s", out_dir)

    df_scored.write.mode("overwrite").parquet(out_dir)

    logger.info("=== combine_data terminé (%d lignes) ===", df_scored.count())
    return out_dir
