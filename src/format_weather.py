"""
format_weather.py
-----------------
Job Spark : raw/open_meteo/weather → formatted/open_meteo/weather
Dépend uniquement de : extract_weather
"""

# TODO : ajouter les imports nécessaires


def format_weather_main(spark=None) -> str:
    """
    Lit weather_raw.json (data/raw/open_meteo/weather/date=.../hour=.../)
    et écrit un Parquet nettoyé dans data/formatted/open_meteo/weather/.

    Structure du JSON brut : liste de points météo, chacun contenant :
      - latitude, longitude, elevation   (niveau racine)
      - current.time                     → weather_time (ISO8601 de la mesure)
      - current.*                        → variables météo

    Colonnes attendues en sortie (ref. Open-Meteo API — paramètre current=) :
      Localisation : latitude, longitude, elevation
      Horodatage   : weather_time  (= current.time)
      Météo        : temperature_2m (°C), relative_humidity_2m (%),
                     wind_speed_10m (km/h), wind_direction_10m (°),
                     wind_gusts_10m (km/h), precipitation (mm),
                     rain (mm), cloud_cover (%), weather_code (WMO 0-99),
                     visibility (m)
      Méta         : extracted_at

    Returns : chemin du répertoire Parquet écrit.
    """
    # TODO : initialiser Spark avec get_spark()
    # TODO : lire weather_raw.json depuis latest_partition("raw", "open_meteo", "weather")
    # TODO : itérer sur chaque point et extraire le bloc current{}
    # TODO : construire les records avec les bons noms de colonnes
    # TODO : créer un DataFrame Spark
    # TODO : écrire en Parquet via output_path("formatted", "open_meteo", "weather")
    raise NotImplementedError("format_weather_main n'est pas encore implémenté")
