"""
serverless_function_call.py
---------------------------
Client d'appel vers la Lambda Scaleway (proxy OpenSky Network).
Contourne le blocage d'IP des hyperscalers par OpenSky.
"""

from typing import Optional

import requests

from helpers import logger

# Bounding box couvrant la France métropolitaine [lat_min, lat_max, lon_min, lon_max]
FRANCE_BOUNDING_BOX = [41.3, 51.1, -5.1, 9.6]

LAMBDA_URL = (
    "https://openskynetworkcallbp0mjxji-opensky-network-call"
    ".functions.fnc.fr-par.scw.cloud/"
)


def fetch_flights_from_lambda() -> Optional[dict]:
    """Appelle la Lambda Scaleway pour récupérer les vols via OpenSky.

    Returns:
        Le dict JSON contenant les données de vols, ou None en cas d'erreur.
    """
    payload = {"bounding_box": FRANCE_BOUNDING_BOX}

    logger.info("Appel de la Lambda via : %s", LAMBDA_URL)

    try:
        response = requests.post(LAMBDA_URL, json=payload, timeout=60)
        response.raise_for_status()

        data = response.json()
        logger.info("Succès ! %s vols récupérés.", data.get("count"))
        return data

    except requests.exceptions.HTTPError as err:
        logger.error("Erreur de l'API Lambda : %s", err)
        logger.error("Détails : %s", response.text)
        return None
    except Exception as err:
        logger.error("Erreur réseau vers la Lambda : %s", err)
        return None
