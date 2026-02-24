"""
helpers.py
----------
Utilitaires partagés par tous les modules du pipeline Sky-Safe :
  - Configuration globale (DATA_ROOT, ES_HOST, etc.)
  - Logger commun
  - Fonctions HTTP génériques (GET, POST)
  - Gestion des chemins du Data Lake (partitions date/heure)
  - Fabrique SparkSession
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, Union

import requests
from pyspark.sql import SparkSession

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ─────────────────────────────────────────────────────────────────────────────

DATA_ROOT: str = os.getenv("DATA_ROOT", "/opt/airflow/data")
ES_HOST: str = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
ES_INDEX: str = os.getenv("ELASTICSEARCH_INDEX", "sky_safe_flights")

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("sky_safe")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────

def http_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> dict:
    """GET HTTP générique. Lève une exception si le statut n'est pas 2xx."""
    logger.info("GET %s | params=%s", url, params)
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def http_post(url: str, data: dict, headers: Optional[dict] = None) -> dict:
    """POST HTTP générique. Lève une exception si le statut n'est pas 2xx."""
    logger.info("POST %s", url)
    response = requests.post(url, data=data, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


# ─────────────────────────────────────────────────────────────────────────────
# DATA LAKE — GESTION DES CHEMINS
# ─────────────────────────────────────────────────────────────────────────────

def build_raw_path(source: str, entity: str, ts: Optional[datetime] = None) -> str:
    """
    Construit et crée le chemin partitionné :
      data/raw/<source>/<entity>/date=YYYY-MM-DD/hour=HH/
    """
    ts = ts or datetime.utcnow()
    path = os.path.join(
        DATA_ROOT, "raw", source, entity,
        "date=" + ts.strftime("%Y-%m-%d"),
        "hour=" + ts.strftime("%H"),
    )
    os.makedirs(path, exist_ok=True)
    return path


def output_path(layer: str, source: str, entity: str) -> str:
    """Construit et crée le dossier de sortie partitionné par date/heure courante."""
    ts = datetime.utcnow()
    path = os.path.join(
        DATA_ROOT, layer, source, entity,
        "date=" + ts.strftime("%Y-%m-%d"),
        "hour=" + ts.strftime("%H"),
    )
    os.makedirs(path, exist_ok=True)
    return path


def latest_partition(layer: str, source: str, entity: str) -> str:
    """
    Retourne le chemin de la partition la plus récente pour une entité.
    Convention : data/<layer>/<source>/<entity>/date=YYYY-MM-DD/hour=HH/
    """
    base = os.path.join(DATA_ROOT, layer, source, entity)
    if not os.path.exists(base):
        raise FileNotFoundError("Aucune donnée disponible dans : " + base)

    date_dirs = sorted(os.listdir(base), reverse=True)
    for date_dir in date_dirs:
        date_path = os.path.join(base, date_dir)
        if not os.path.isdir(date_path):
            continue
        hour_dirs = sorted(os.listdir(date_path), reverse=True)
        if hour_dirs:
            return os.path.join(date_path, hour_dirs[0])

    raise FileNotFoundError("Aucune partition trouvée dans : " + base)


# ─────────────────────────────────────────────────────────────────────────────
# JSON
# ─────────────────────────────────────────────────────────────────────────────

def save_json(data: Union[dict, list], filepath: str) -> None:
    """Sauvegarde un objet Python en JSON indenté."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Fichier sauvegardé : %s", filepath)


# ─────────────────────────────────────────────────────────────────────────────
# SPARK
# ─────────────────────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    """Crée ou récupère la SparkSession locale (optimisée pour conteneur Docker)."""
    spark = (
        SparkSession.builder
        .appName("SkySafe-Pipeline")
        .master("local[1]")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
