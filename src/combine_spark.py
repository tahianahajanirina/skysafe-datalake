"""
combine_spark.py
----------------
Job Spark : formatted → enriched
Jointure spatiale vols × météo + calcul du Score de Risque.
"""

# TODO : ajouter les imports nécessaires


# ─────────────────────────────────────────────────────────────────────────────
# TODO : définir une UDF Spark pour calculer la distance Haversine (km)
#        entre deux points géographiques (lat1, lon1, lat2, lon2)
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# JOB COMBINE
# ─────────────────────────────────────────────────────────────────────────────

def combine_data_main(spark=None) -> str:
    """
    Jointure spatiale vols × météo et calcul du Score de Risque.

    Étapes à implémenter :
      1. Lire les Parquet formatés :
           - data/formatted/opensky/flights/    via latest_partition()
           - data/formatted/open_meteo/weather/ via latest_partition()
      2. Cross join vols × points météo, calculer la distance Haversine
         et ne garder que le point météo le plus proche de chaque vol
         (Window partitionné sur icao24, trié par dist_km)
      3. Calculer un score de risque (entier 0–100) selon les critères :
           - Orage (weather_code >= 95)         : +40 pts
           - Rafales > 80 km/h                  : +25 pts  | > 50 km/h : +10 pts
           - Précipitations > 5 mm              : +20 pts  | > 0 mm    : +10 pts
           - Visibilité < 1 000 m               : +20 pts  | < 3 000 m : +10 pts
           - Couverture nuageuse > 80%          : +10 pts  | > 50%     : +5 pts
           - Altitude baro < 300 m (en vol)     : +15 pts
      4. Ajouter une colonne risk_category : LOW / MEDIUM / HIGH
           - HIGH   : score >= 60
           - MEDIUM : score >= 30
           - LOW    : sinon
      5. Écrire en Parquet dans data/enriched/sky_safe/flights_weather/

    Returns : chemin du répertoire Parquet écrit.
    """
    # TODO : initialiser Spark avec get_spark()
    # TODO : lire les deux Parquet formatés
    # TODO : préfixer les colonnes météo (w_lat, w_lon) pour éviter les collisions
    # TODO : cross join + colonne dist_km via l'UDF Haversine
    # TODO : fenêtre Window par icao24 triée par dist_km → garder rank == 1
    # TODO : calculer risk_score (cast IntegerType)
    # TODO : calculer risk_category
    # TODO : écrire en Parquet via output_path("enriched", "sky_safe", "flights_weather")
    raise NotImplementedError("combine_data_main n'est pas encore implémenté")
