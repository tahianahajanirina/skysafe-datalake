"""
format_flights.py
-----------------
Job Spark : raw/opensky/flights → formatted/opensky/flights
Dépend uniquement de : extract_flights
"""

import json
import os
from typing import Any, Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, IntegerType
)

from helpers import get_spark, latest_partition, output_path, logger


def _safe_get(state: list, idx: int):
    """Retourne state[idx] si dispo, sinon None."""
    if not isinstance(state, list):
        return None
    return state[idx] if idx < len(state) else None


def _to_float(value) -> Optional[float]:
    """Cast en float si possible, sinon None (évite le rejet PySpark int→DoubleType)."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _clean_callsign(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def format_flights_main(spark=None) -> str:
    """
    Lit flights_raw.json (data/raw/opensky/flights/date=.../hour=.../)
    et écrit un Parquet nettoyé dans data/formatted/opensky/flights/.
    """
    logger.info("=== Démarrage format_flights ===")

    if spark is None:
        spark = get_spark()

    # 1) Trouver le dernier dossier raw
    raw_dir = latest_partition("raw", "opensky", "flights")
    raw_file = os.path.join(raw_dir, "flights_raw.json")

    logger.info("Lecture raw flights depuis: %s", raw_file)

    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"Fichier introuvable: {raw_file}")

    # 2) Lire le JSON brut
    with open(raw_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    observation_time = payload.get("time")          # timestamp global (epoch sec)
    extracted_at = payload.get("_extracted_at")     # ISO string ajouté à l'extract
    states = payload.get("states") or []

    logger.info("Nombre de states reçus: %d", len(states))

    # 3) Mapper chaque state[] en dict
    records: List[Dict[str, Any]] = []

    for state in states:
        rec = {
            # Identité
            "icao24": _safe_get(state, 0),
            "callsign": _clean_callsign(_safe_get(state, 1)),
            "origin_country": _safe_get(state, 2),

            # Horodatage
            "observation_time_epoch": observation_time,
            "time_position_epoch": _safe_get(state, 3),
            "last_contact_epoch": _safe_get(state, 4),

            # Position
            "longitude": _to_float(_safe_get(state, 5)),
            "latitude": _to_float(_safe_get(state, 6)),
            "baro_altitude": _to_float(_safe_get(state, 7)),
            "on_ground": _safe_get(state, 8),
            "geo_altitude": _to_float(_safe_get(state, 13)),

            # Cinématique
            "velocity": _to_float(_safe_get(state, 9)),
            "true_track": _to_float(_safe_get(state, 10)),
            "vertical_rate": _to_float(_safe_get(state, 11)),

            # Transpondeur
            "squawk": _safe_get(state, 14),
            "position_source": _safe_get(state, 16),

            # Méta
            "extracted_at_str": extracted_at,
        }
        records.append(rec)

    # 4) Schéma explicite (robuste même si records vide)
    schema = StructType([
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),

        StructField("observation_time_epoch", IntegerType(), True),
        StructField("time_position_epoch", IntegerType(), True),
        StructField("last_contact_epoch", IntegerType(), True),

        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("geo_altitude", DoubleType(), True),

        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),

        StructField("squawk", StringType(), True),
        StructField("position_source", IntegerType(), True),

        StructField("extracted_at_str", StringType(), True),
    ])

    df = spark.createDataFrame(records, schema=schema)

    # 5) Nettoyage minimum obligatoire
    df = df.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())

    # 6) Conversion timestamps (epoch -> timestamp)
    df = (
        df
        .withColumn("observation_time", F.to_timestamp(F.from_unixtime(F.col("observation_time_epoch"))))
        .withColumn("time_position", F.to_timestamp(F.from_unixtime(F.col("time_position_epoch"))))
        .withColumn("last_contact", F.to_timestamp(F.from_unixtime(F.col("last_contact_epoch"))))
        .withColumn("extracted_at", F.to_timestamp("extracted_at_str"))
        .drop("observation_time_epoch", "time_position_epoch", "last_contact_epoch", "extracted_at_str")
    )

    # (Optionnel) cast supplémentaire / normalisation
    # position_source -> label lisible (garde aussi l'int)
    df = df.withColumn(
        "position_source_label",
        F.when(F.col("position_source") == 0, F.lit("ADS-B"))
         .when(F.col("position_source") == 1, F.lit("ASTERIX"))
         .when(F.col("position_source") == 2, F.lit("MLAT"))
         .when(F.col("position_source") == 3, F.lit("FLARM"))
         .otherwise(F.lit(None))
    )

    # 7) Écriture parquet
    out_dir = output_path("formatted", "opensky", "flights")
    logger.info("Écriture Parquet vers: %s", out_dir)

    df.write.mode("overwrite").parquet(out_dir)

    logger.info("=== format_flights terminé (%d lignes) ===", df.count())
    return out_dir