"""
extract_weather.py
------------------
Extraction des données météo depuis l'API Open-Meteo.
Sauvegarde en JSON dans : data/raw/open_meteo/weather/date=.../hour=.../
"""

import os
from datetime import datetime
from typing import Dict, List, Optional

import requests

from helpers import (
    http_get,
    build_raw_path,
    save_json,
    logger,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION (importée depuis helpers, surchargeable ici si besoin)
# ─────────────────────────────────────────────────────────────────────────────

WEATHER_BASE_URL: str = os.getenv(
    "WEATHER_BASE_URL", "https://api.open-meteo.com/v1/forecast"
)
# Variables courantes demandées à Open-Meteo (toutes disponibles en `current`)
# Ref: https://open-meteo.com/en/docs
WEATHER_VARIABLES: str = (
    "temperature_2m,"
    "relative_humidity_2m,"
    "wind_speed_10m,"
    "wind_direction_10m,"
    "wind_gusts_10m,"
    "precipitation,"
    "rain,"
    "cloud_cover,"
    "weather_code,"
    "visibility"
)

DEFAULT_WEATHER_POINTS: List[Dict] = [
    {"latitude": 48.709632, "longitude": 2.208563},   # Paris CDG
    {"latitude": 43.629421, "longitude": 1.367789},   # Toulouse
    {"latitude": 45.726009, "longitude": 5.090928},   # Lyon
    {"latitude": 43.434242, "longitude": 5.212784},   # Marseille
    {"latitude": 47.460152, "longitude": -0.529704},  # Nantes
    {"latitude": 50.561237, "longitude": 3.086957},   # Lille
]


# ─────────────────────────────────────────────────────────────────────────────
# FONCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_weather_for_point(latitude: float, longitude: float) -> dict:
    """Récupère les données météo courantes pour un point (lat, lon)."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": WEATHER_VARIABLES,
    }
    data = http_get(WEATHER_BASE_URL, params=params)
    data["_extracted_at"] = datetime.utcnow().isoformat()
    return data


def fetch_weather(weather_points: Optional[List[Dict]] = None) -> str:
    """
    Récupère la météo pour tous les points de la grille.
    Sauvegarde dans data/raw/open_meteo/weather/.

    Returns : chemin du fichier JSON.
    """
    points = weather_points or DEFAULT_WEATHER_POINTS
    all_results = []

    for point in points:
        lat, lon = point["latitude"], point["longitude"]
        try:
            result = fetch_weather_for_point(lat, lon)
            all_results.append(result)
        except requests.HTTPError as exc:
            logger.warning("Erreur météo pour (%.4f, %.4f) : %s", lat, lon, exc)

    ts = datetime.utcnow()
    output_dir = build_raw_path("open_meteo", "weather", ts)
    filepath = os.path.join(output_dir, "weather_raw.json")
    save_json(all_results, filepath)

    logger.info("Données météo extraites pour %d points", len(all_results))
    return filepath


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE AIRFLOW
# ─────────────────────────────────────────────────────────────────────────────

def extract_weather_main() -> None:
    """Point d'entrée pour la tâche Airflow 'extract_weather_api'."""
    logger.info("=== Démarrage extraction MÉTÉO ===")
    filepath = fetch_weather()
    logger.info("=== Extraction MÉTÉO terminée → %s ===", filepath)
