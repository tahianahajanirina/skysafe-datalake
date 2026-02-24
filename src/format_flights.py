"""
format_flights.py
-----------------
Job Spark : raw/opensky/flights → formatted/opensky/flights
Dépend uniquement de : extract_flights
"""

# TODO : ajouter les imports nécessaires


def format_flights_main(spark=None) -> str:
    """
    Lit flights_raw.json (data/raw/opensky/flights/date=.../hour=.../)
    et écrit un Parquet nettoyé dans data/formatted/opensky/flights/.

    Colonnes attendues en sortie (ref. OpenSky REST API — /states/all) :
      Identifiant  : icao24 [0], callsign [1], origin_country [2]
      Horodatage   : observation_time (racine), time_position [3], last_contact [4]
      Position     : longitude [5], latitude [6], baro_altitude [7],
                     geo_altitude [13], on_ground [8]
      Cinématique  : velocity [9], true_track [10], vertical_rate [11]
      Transpondeur : squawk [14], position_source [16]
                     (position_source : 0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM)
      Méta         : extracted_at

    Contraintes :
      - Filtrer les vols sans position (latitude ou longitude null)
      - Nettoyer le callsign (strip des espaces)

    Returns : chemin du répertoire Parquet écrit.
    """
    # TODO : initialiser Spark avec get_spark()
    # TODO : lire flights_raw.json depuis latest_partition("raw", "opensky", "flights")
    # TODO : extraire la liste states[] et le timestamp racine time
    # TODO : construire les records en mappant chaque state[i] sur les bons noms de colonnes
    # TODO : créer un DataFrame Spark, filtrer lat/lon null
    # TODO : écrire en Parquet via output_path("formatted", "opensky", "flights")
    raise NotImplementedError("format_flights_main n'est pas encore implémenté")
