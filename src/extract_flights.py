"""
extract_flights.py
------------------
Extraction des données de vols depuis l'API OpenSky Network.
Authentification OAuth2 (client_credentials) + GET /states/all.
Sauvegarde en JSON dans : data/raw/opensky/flights/date=.../hour=.../
"""

import os
from datetime import datetime

from helpers import (
    http_get,
    http_post,
    build_raw_path,
    save_json,
    logger,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

SKY_NETWORK_BASE_URL: str = os.getenv(
    "SKY_NETWORK_BASE_URL", "https://opensky-network.org/api"
)
SKY_NETWORK_TOKEN_URL: str = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
SKY_NETWORK_CLIENT_ID: str = os.getenv(
    "SKY_NETWORK_CLIENT_ID", "tahiana-api-client"
)
SKY_NETWORK_CLIENT_SECRET: str = os.getenv(
    "SKY_NETWORK_CLIENT_SECRET", "OfiQ7YzZIJyBLAaqKHYb0BMx3LXXKKwK"
)


# ─────────────────────────────────────────────────────────────────────────────
# AUTHENTIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def get_opensky_token() -> str:
    """Récupère un Bearer token via OAuth2 client_credentials."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": SKY_NETWORK_CLIENT_ID,
        "client_secret": SKY_NETWORK_CLIENT_SECRET,
    }
    token_data = http_post(SKY_NETWORK_TOKEN_URL, data=payload)
    token = token_data["access_token"]
    logger.info("Token OpenSky obtenu (expire dans %s s)", token_data.get("expires_in"))
    return token


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def fetch_flights(token: str) -> str:
    """
    GET /states/all avec Bearer token.
    Sauvegarde dans data/raw/opensky/flights/.

    Returns : chemin du fichier JSON.
    """
    url = SKY_NETWORK_BASE_URL + "/states/all"
    headers = {"Authorization": "Bearer " + token}
    raw_data = http_get(url, headers=headers)

    raw_data["_extracted_at"] = datetime.utcnow().isoformat()

    ts = datetime.utcnow()
    output_dir = build_raw_path("opensky", "flights", ts)
    filepath = os.path.join(output_dir, "flights_raw.json")
    save_json(raw_data, filepath)

    nb_flights = len(raw_data.get("states") or [])
    logger.info("Vols extraits : %d", nb_flights)
    return filepath


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE AIRFLOW
# ─────────────────────────────────────────────────────────────────────────────

def extract_flights_main() -> None:
    """Point d'entrée pour la tâche Airflow 'extract_flights_api'."""
    logger.info("=== Démarrage extraction VOLS ===")
    token = get_opensky_token()
    filepath = fetch_flights(token)
    logger.info("=== Extraction VOLS terminée → %s ===", filepath)
