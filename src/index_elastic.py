"""
index_elastic.py
----------------
Job : enriched → usage → Elasticsearch
Prépare la couche Usage et indexe les documents dans Elasticsearch.
"""

from datetime import datetime

from pyspark.sql import functions as F

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from helpers import get_spark, latest_partition, output_path, ES_HOST, ES_INDEX, logger


# ─────────────────────────────────────────────────────────────────────────────
# MAPPING ELASTICSEARCH (geo_point obligatoire pour Kibana Maps)
# ─────────────────────────────────────────────────────────────────────────────

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            # Identifiants vol
            "icao24":          {"type": "keyword"},
            "callsign":        {"type": "keyword"},
            "origin_country":  {"type": "keyword"},
            # Position géographique — geo_point pour Kibana Maps
            "location":        {"type": "geo_point"},
            "baro_altitude":   {"type": "float"},
            "geo_altitude":    {"type": "float"},
            "on_ground":       {"type": "boolean"},
            # Cinématique
            "velocity":        {"type": "float"},
            "true_track":      {"type": "float"},
            "vertical_rate":   {"type": "float"},
            # Horodatages
            "observation_time": {"type": "date"},
            "extracted_at":     {"type": "date"},
            # Météo
            "wind_speed_10m":      {"type": "float"},
            "wind_direction_10m":  {"type": "float"},
            "wind_gusts_10m":      {"type": "float"},
            "precipitation":       {"type": "float"},
            "rain":                {"type": "float"},
            "cloud_cover":         {"type": "integer"},
            "weather_code":        {"type": "integer"},
            "visibility":          {"type": "float"},
            "temperature_2m":      {"type": "float"},
            # Score de risque
            "risk_score":    {"type": "integer"},
            "risk_category": {"type": "keyword"},
            # Machine Learning — Phase de vol (K-Means)
            "flight_phase":    {"type": "keyword"},
            "flight_phase_id": {"type": "integer"},
            # Machine Learning — Détection d'anomalies
            "is_anomaly":    {"type": "boolean"},
            "anomaly_score": {"type": "float"},
        }
    }
}


# ─────────────────────────────────────────────────────────────────────────────
# COUCHE USAGE (enriched → usage)
# ─────────────────────────────────────────────────────────────────────────────

def prepare_usage(spark=None) -> str:
    """
    Lit le Parquet enrichi (data/enriched/sky_safe/flights_weather/)
    et sélectionne / renomme les colonnes utiles pour Kibana.
    Écrit en Parquet dans data/usage/sky_safe/dashboard/.

    Returns : chemin du répertoire Parquet écrit.
    """
    logger.info("=== Démarrage prepare_usage ===")

    if spark is None:
        spark = get_spark()

    # 1. Lecture de la dernière partition enrichie
    enriched_dir = latest_partition("enriched", "sky_safe", "flights_weather")
    logger.info("Lecture enriched depuis : %s", enriched_dir)
    df = spark.read.parquet(enriched_dir)

    # 2. Sélection et renommage des colonnes (w_ → noms propres pour ES)
    df_usage = df.select(
        # Vol
        F.col("icao24"),
        F.col("callsign"),
        F.col("origin_country"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("baro_altitude"),
        F.col("geo_altitude"),
        F.col("velocity"),
        F.col("true_track"),
        F.col("vertical_rate"),
        F.col("on_ground"),
        F.col("observation_time"),
        # Météo (renommage w_ → nom propre)
        F.col("w_wind_speed_10m").alias("wind_speed_10m"),
        F.col("w_wind_direction_10m").alias("wind_direction_10m"),
        F.col("w_wind_gusts_10m").alias("wind_gusts_10m"),
        F.col("w_precipitation").alias("precipitation"),
        F.col("w_rain").alias("rain"),
        F.col("w_cloud_cover").alias("cloud_cover"),
        F.col("w_weather_code").alias("weather_code"),
        F.col("w_visibility").alias("visibility"),
        F.col("w_temperature_2m").alias("temperature_2m"),
        # Score
        F.col("risk_score"),
        F.col("risk_category"),
        # Machine Learning — Phase de vol
        F.col("flight_phase"),
        F.col("flight_phase_id"),
        # Machine Learning — Anomalies
        F.col("is_anomaly"),
        F.col("anomaly_score"),
        # Méta
        F.col("extracted_at"),
    )

    # 3. Écriture Parquet (usage)
    out_dir = output_path("usage", "sky_safe", "dashboard")
    logger.info("Écriture usage Parquet vers : %s", out_dir)
    df_usage.write.mode("overwrite").parquet(out_dir)

    logger.info("=== prepare_usage terminé (%d lignes) ===", df_usage.count())
    return out_dir


# ─────────────────────────────────────────────────────────────────────────────
# INDEXATION ELASTICSEARCH
# ─────────────────────────────────────────────────────────────────────────────

def _row_to_es_doc(row_dict: dict) -> dict:
    """
    Transforme un dict PySpark en document Elasticsearch.
    - Fusionne latitude/longitude en un champ 'location' (geo_point).
    - Convertit les timestamps en ISO 8601 pour ES.
    """
    doc = dict(row_dict)

    # geo_point pour Kibana Maps
    lat = doc.pop("latitude", None)
    lon = doc.pop("longitude", None)
    if lat is not None and lon is not None:
        doc["location"] = {"lat": float(lat), "lon": float(lon)}

    # Conversion timestamps → ISO string
    for ts_field in ("observation_time", "extracted_at"):
        val = doc.get(ts_field)
        if val is not None and isinstance(val, datetime):
            doc[ts_field] = val.isoformat()

    return doc


def index_to_elasticsearch(spark=None) -> None:
    """
    Lit la couche Usage (data/usage/sky_safe/dashboard/)
    et effectue un bulk insert dans l'index Elasticsearch ES_INDEX.
    """
    logger.info("=== Démarrage index_to_elasticsearch ===")

    if spark is None:
        spark = get_spark()

    # 1. Lecture de la dernière partition usage
    usage_dir = latest_partition("usage", "sky_safe", "dashboard")
    logger.info("Lecture usage depuis : %s", usage_dir)
    df = spark.read.parquet(usage_dir)

    # 2. Conversion en liste de dicts Python
    rows = [row.asDict() for row in df.collect()]
    logger.info("Documents à indexer : %d", len(rows))

    if not rows:
        logger.warning("Aucun document à indexer — abandon.")
        return

    # 3. Connexion Elasticsearch
    logger.info("Connexion à Elasticsearch : %s", ES_HOST)
    es = Elasticsearch(ES_HOST)

    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch sur {ES_HOST}")
    logger.info("Elasticsearch connecté.")

    # 4. Création de l'index avec mapping (si inexistant)
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, body=INDEX_MAPPING)
        logger.info("Index '%s' créé avec mapping.", ES_INDEX)
    else:
        logger.info("Index '%s' existe déjà.", ES_INDEX)

    # 5. Construction des actions bulk
    actions = []
    for row_dict in rows:
        doc = _row_to_es_doc(row_dict)
        actions.append({
            "_index":  ES_INDEX,
            "_id":     doc.get("icao24"),
            "_source": doc,
        })

    # 6. Bulk insert
    success, errors = bulk(es, actions, raise_on_error=False)
    logger.info("Elasticsearch bulk : %d insérés, %d erreurs", success, len(errors))

    if errors:
        for err in errors[:10]:  # afficher les 10 premières erreurs max
            logger.error("  ES erreur : %s", err)

    logger.info("=== index_to_elasticsearch terminé ===")


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE AIRFLOW
# ─────────────────────────────────────────────────────────────────────────────

def index_to_elastic_main() -> None:
    """Point d'entrée Airflow : couche Usage + indexation Elasticsearch."""
    logger.info("=== Pipeline indexation : usage → Elasticsearch ===")
    spark = get_spark()
    prepare_usage(spark)
    index_to_elasticsearch(spark)
    logger.info("=== Pipeline indexation terminé ===")
