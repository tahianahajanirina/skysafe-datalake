"""
helpers.py
----------
Utilitaires partagés par tous les modules du pipeline Sky-Safe :
  - Configuration globale (S3, ES, etc.)
  - Logger commun
  - Fonctions HTTP génériques (GET, POST)
  - Gestion des chemins du Data Lake sur Amazon S3
  - I/O JSON (S3 via boto3)
  - Fabrique SparkSession (avec support S3A)
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, Union

import boto3
import requests
from pyspark.sql import SparkSession

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ─────────────────────────────────────────────────────────────────────────────

ES_HOST: str = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
ES_INDEX: str = os.getenv("ELASTICSEARCH_INDEX", "sky_safe_flights")
S3_BUCKET: str = os.getenv("S3_BUCKET", "data705-opensky-datalake")

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("sky_safe")


# ─────────────────────────────────────────────────────────────────────────────
# S3 — CREDENTIALS & CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def get_s3_creds() -> dict:
    """Retourne les credentials S3 depuis les variables d'environnement."""
    return {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "region_name": os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
    }


def get_s3_client():
    """Retourne un client boto3 S3."""
    creds = get_s3_creds()
    return boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        region_name=creds["region_name"],
    )


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
# DATA LAKE — CHEMINS S3
# ─────────────────────────────────────────────────────────────────────────────

def _s3_uri(key: str) -> str:
    """Construit l'URI s3a:// complète à partir d'une clé S3."""
    return f"s3a://{S3_BUCKET}/{key}"


def _parse_s3_uri(uri: str) -> tuple:
    """Extrait (bucket, key) depuis une URI s3a:// ou s3://."""
    path = uri.replace("s3a://", "").replace("s3://", "")
    parts = path.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def join_path(base: str, *parts: str) -> str:
    """Joint des segments de chemin — fonctionne avec les URIs S3 et les chemins locaux."""
    if base.startswith("s3"):
        return base.rstrip("/") + "/" + "/".join(p.strip("/") for p in parts)
    return os.path.join(base, *parts)


def build_raw_path(source: str, entity: str, ts: Optional[datetime] = None) -> str:
    """
    Construit le chemin S3 partitionné pour les données brutes :
      s3a://bucket/raw/<source>/<entity>/date=YYYY-MM-DD/hour=HH/
    """
    ts = ts or datetime.utcnow()
    key = f"raw/{source}/{entity}/date={ts:%Y-%m-%d}/hour={ts:%H}"
    return _s3_uri(key)


def output_path(layer: str, source: str, entity: str) -> str:
    """Construit le chemin S3 de sortie partitionné par date/heure courante."""
    ts = datetime.utcnow()
    key = f"{layer}/{source}/{entity}/date={ts:%Y-%m-%d}/hour={ts:%H}"
    return _s3_uri(key)


def latest_partition(layer: str, source: str, entity: str) -> str:
    """
    Retourne le chemin S3 de la partition la plus récente pour une entité.
    Parcourt les préfixes date= puis hour= via l'API S3 ListObjectsV2.
    """
    s3 = get_s3_client()
    prefix = f"{layer}/{source}/{entity}/"

    # Lister les préfixes date=
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")
    date_prefixes = sorted(
        [cp["Prefix"] for cp in resp.get("CommonPrefixes", [])],
        reverse=True,
    )

    if not date_prefixes:
        raise FileNotFoundError(f"Aucune donnée disponible dans : {_s3_uri(prefix)}")

    for date_prefix in date_prefixes:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=date_prefix, Delimiter="/")
        hour_prefixes = sorted(
            [cp["Prefix"] for cp in resp.get("CommonPrefixes", [])],
            reverse=True,
        )
        if hour_prefixes:
            latest = hour_prefixes[0].rstrip("/")
            return _s3_uri(latest)

    raise FileNotFoundError(f"Aucune partition trouvée dans : {_s3_uri(prefix)}")


# ─────────────────────────────────────────────────────────────────────────────
# JSON — I/O S3
# ─────────────────────────────────────────────────────────────────────────────

def save_json(data: Union[dict, list], filepath: str) -> None:
    """Sauvegarde un objet Python en JSON sur S3 (ou localement si chemin local)."""
    body = json.dumps(data, ensure_ascii=False, indent=2)
    if filepath.startswith("s3"):
        bucket, key = _parse_s3_uri(filepath)
        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket, Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
    else:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(body)
    logger.info("Fichier sauvegardé : %s", filepath)


def read_json(filepath: str):
    """Lit un fichier JSON depuis S3 (ou localement si chemin local)."""
    if filepath.startswith("s3"):
        bucket, key = _parse_s3_uri(filepath)
        s3 = get_s3_client()
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    logger.info("Fichier lu : %s", filepath)
    return data


# ─────────────────────────────────────────────────────────────────────────────
# SPARK (avec support S3A)
# ─────────────────────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    """Crée ou récupère la SparkSession locale avec support S3A."""
    creds = get_s3_creds()
    region = creds.get("region_name", "eu-north-1")
    spark = (
        SparkSession.builder
        .appName("SkySafe-Pipeline")
        .master("local[1]")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        # ── S3A ──
        .config("spark.jars",
                "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
                "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",
                creds["aws_access_key_id"])
        .config("spark.hadoop.fs.s3a.secret.key",
                creds["aws_secret_access_key"])
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{region}.amazonaws.com")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
