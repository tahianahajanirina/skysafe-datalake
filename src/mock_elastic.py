"""
mock_elastic.py
---------------
Injecte des données fictives dans Elasticsearch pour tester le pipeline
sans avoir besoin d'exécuter les étapes Extract/Transform.

Structure des documents identique à celle produite par index_elastic.py
(couche usage → sky_safe_flights).
"""

import random
from datetime import datetime, timezone
from typing import Dict, List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# ─── Constantes alignées sur helpers.py ───────────────────────────────────────
ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "sky_safe_flights"

# ─── Mapping explicite (geo_point obligatoire pour les cartes Kibana) ──────────
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
        }
    }
}

# ─── Données réalistes pour la génération ─────────────────────────────────────
CALLSIGN_PREFIXES = ["AFR", "EZY", "RYR", "DLH", "BAW", "IBE", "KLM", "SWR"]
COUNTRIES = ["France", "Germany", "United Kingdom", "Spain", "Netherlands",
             "Italy", "Belgium", "Switzerland"]

# Zones géographiques en Europe (lat_min, lat_max, lon_min, lon_max)
EUROPE_BBOX = (42.0, 51.5, -5.0, 15.0)


def _random_icao24() -> str:
    return "".join(random.choices("0123456789abcdef", k=6))


def _risk_category(score: int) -> str:
    if score >= 60:
        return "HIGH"
    if score >= 30:
        return "MEDIUM"
    return "LOW"


def _compute_risk_score(wind_gusts: float, precipitation: float,
                        visibility: float, cloud_cover: int,
                        weather_code: int, baro_altitude: float,
                        on_ground: bool) -> int:
    """Même logique que combine_spark.py (à garder synchronisée)."""
    score = 0
    if weather_code >= 95:
        score += 40
    if wind_gusts > 80:
        score += 25
    elif wind_gusts > 50:
        score += 10
    if precipitation > 5:
        score += 20
    elif precipitation > 0:
        score += 10
    if visibility < 1_000:
        score += 20
    elif visibility < 3_000:
        score += 10
    if cloud_cover > 80:
        score += 10
    elif cloud_cover > 50:
        score += 5
    if not on_ground and baro_altitude < 300:
        score += 15
    return min(score, 100)


def generate_mock_documents(n: int = 20) -> List[Dict]:
    """Génère n documents fictifs au format exact de la couche usage."""
    lat_min, lat_max, lon_min, lon_max = EUROPE_BBOX
    docs = []
    now = datetime.now(timezone.utc).isoformat()

    for _ in range(n):
        on_ground = random.random() < 0.1
        lat = round(random.uniform(lat_min, lat_max), 4)
        lon = round(random.uniform(lon_min, lon_max), 4)
        baro_alt = round(random.uniform(0, 12_000), 1)
        wind_gusts = round(random.uniform(0, 130), 1)
        precipitation = round(random.uniform(0, 20), 1)
        visibility = round(random.uniform(200, 10_000), 0)
        cloud_cover = random.randint(0, 100)
        weather_code = random.choice([0, 1, 2, 3, 51, 61, 71, 80, 95, 99])

        risk_score = _compute_risk_score(
            wind_gusts, precipitation, visibility,
            cloud_cover, weather_code, baro_alt, on_ground
        )

        docs.append({
            # Vol
            "icao24":           _random_icao24(),
            "callsign":         random.choice(CALLSIGN_PREFIXES) + str(random.randint(100, 9999)),
            "origin_country":   random.choice(COUNTRIES),
            "location":         {"lat": lat, "lon": lon},   # geo_point
            "baro_altitude":    baro_alt,
            "geo_altitude":     round(baro_alt + random.uniform(-50, 50), 1),
            "on_ground":        on_ground,
            "velocity":         round(random.uniform(0, 950), 1),
            "true_track":       round(random.uniform(0, 360), 1),
            "vertical_rate":    round(random.uniform(-20, 20), 2),
            "observation_time": now,
            # Météo
            "wind_speed_10m":     round(wind_gusts * 0.75, 1),
            "wind_direction_10m": round(random.uniform(0, 360), 1),
            "wind_gusts_10m":     wind_gusts,
            "precipitation":      precipitation,
            "rain":               round(precipitation * random.uniform(0, 1), 1),
            "cloud_cover":        cloud_cover,
            "weather_code":       weather_code,
            "visibility":         visibility,
            "temperature_2m":     round(random.uniform(-10, 35), 1),
            # Score
            "risk_score":    risk_score,
            "risk_category": _risk_category(risk_score),
            # Méta
            "extracted_at": now,
        })

    return docs


def inject_fake_data(n: int = 20) -> None:
    # 1. Connexion
    print(f"Connexion à Elasticsearch ({ES_HOST})...")
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch sur {ES_HOST}")
    print("Connecté.")

    # 2. Création de l'index avec mapping (ignoré si l'index existe déjà)
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, body=INDEX_MAPPING)
        print(f"Index '{ES_INDEX}' créé avec mapping.")
    else:
        print(f"Index '{ES_INDEX}' existe déjà.")

    # 3. Génération des documents
    docs = generate_mock_documents(n)
    print(f"Génération de {len(docs)} documents fictifs...")

    # 4. Bulk insert (identique à ce que fera index_elastic.py)
    actions = [
        {
            "_index": ES_INDEX,
            "_id":    doc["icao24"],   # icao24 comme identifiant unique
            "_source": doc,
        }
        for doc in docs
    ]
    success, errors = bulk(es, actions, raise_on_error=False)
    print(f"Insérés : {success} | Erreurs : {len(errors)}")

    if errors:
        for err in errors:
            print(f"  ERREUR : {err}")

    # 5. Résumé par catégorie de risque
    categories = {"LOW": 0, "MEDIUM": 0, "HIGH": 0}
    for doc in docs:
        categories[doc["risk_category"]] += 1
    print(f"Répartition : LOW={categories['LOW']}  MEDIUM={categories['MEDIUM']}  HIGH={categories['HIGH']}")


if __name__ == "__main__":
    inject_fake_data(n=1000)