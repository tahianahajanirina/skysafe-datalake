"""
index_elastic.py
----------------
Job : enriched → usage → Elasticsearch
Prépare la couche Usage et indexe les documents dans Elasticsearch.
"""

# TODO : ajouter les imports nécessaires


# ─────────────────────────────────────────────────────────────────────────────
# COUCHE USAGE (enriched → usage)
# ─────────────────────────────────────────────────────────────────────────────

def prepare_usage(spark=None) -> str:
    """
    Lit le Parquet enrichi (data/enriched/sky_safe/flights_weather/)
    et sélectionne les colonnes utiles pour Kibana.
    Écrit en Parquet dans data/usage/sky_safe/dashboard/.

    Colonnes à sélectionner :
      Vol    : icao24, callsign, origin_country, latitude, longitude,
               baro_altitude, geo_altitude, velocity, true_track,
               vertical_rate, on_ground, observation_time
      Météo  : wind_speed_10m, wind_direction_10m, wind_gusts_10m,
               precipitation, rain, cloud_cover, weather_code,
               visibility, temperature_2m
      Score  : risk_score, risk_category
      Méta   : extracted_at

    Returns : chemin du répertoire Parquet écrit.
    """
    # TODO : initialiser Spark avec get_spark()
    # TODO : lire latest_partition("enriched", "sky_safe", "flights_weather")
    # TODO : sélectionner les colonnes listées ci-dessus
    # TODO : écrire en Parquet via output_path("usage", "sky_safe", "dashboard")
    raise NotImplementedError("prepare_usage n'est pas encore implémenté")


# ─────────────────────────────────────────────────────────────────────────────
# INDEXATION ELASTICSEARCH
# ─────────────────────────────────────────────────────────────────────────────

def index_to_elasticsearch(spark=None) -> None:
    """
    Lit la couche Usage (data/usage/sky_safe/dashboard/)
    et effectue un bulk insert dans l'index Elasticsearch ES_INDEX.

    Chaque document doit avoir :
      - _index  : ES_INDEX
      - _id     : icao24 (identifiant unique du vol)
      - _source : le dict complet du document

    Utiliser helpers.bulk() pour l'insertion en masse.
    Vérifier la connexion avec es.ping() avant l'envoi.
    """
    # TODO : initialiser Spark avec get_spark()
    # TODO : lire latest_partition("usage", "sky_safe", "dashboard")
    # TODO : convertir le DataFrame en liste de dicts (row.asDict())
    # TODO : créer le client Elasticsearch(ES_HOST) et vérifier es.ping()
    # TODO : construire la liste d'actions pour helpers.bulk()
    # TODO : appeler helpers.bulk() et logger le résultat (succès + erreurs)
    raise NotImplementedError("index_to_elasticsearch n'est pas encore implémenté")


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE AIRFLOW
# ─────────────────────────────────────────────────────────────────────────────

def index_to_elastic_main() -> None:
    """Point d'entrée Airflow : couche Usage + indexation Elasticsearch."""
    # TODO : initialiser Spark une seule fois et le passer aux deux fonctions
    # TODO : appeler prepare_usage(spark) puis index_to_elasticsearch(spark)
    raise NotImplementedError("index_to_elastic_main n'est pas encore implémenté")
