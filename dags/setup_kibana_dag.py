"""
setup_kibana_dag.py
-------------------
DAG one-shot : attend que l'index Elasticsearch contienne des données,
puis importe automatiquement le dashboard Kibana.
S'exécute une seule fois (@once).
"""

import sys

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(1, ".")
sys.path.insert(2, "./src")

import os
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from setup_kibana import importer_dashboard_kibana


# ─── Configuration ────────────────────────────────────────────────────────────
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "sky_safe_flights")


# ─── Sensor : attendre que l'index ES existe et contienne des docs ────────────
def _check_es_index_has_data() -> bool:
    """Retourne True si l'index ES existe et contient au moins 1 document."""
    try:
        url = f"{ES_HOST}/{ES_INDEX}/_count"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            count = resp.json().get("count", 0)
            if count > 0:
                print(f"Index '{ES_INDEX}' contient {count} document(s). OK.")
                return True
            print(f"Index '{ES_INDEX}' existe mais est vide.")
        else:
            print(f"Index '{ES_INDEX}' introuvable (HTTP {resp.status_code}).")
    except Exception as exc:
        print(f"Elasticsearch non joignable : {exc}")
    return False


# ─── DAG ──────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "sky_safe_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 23),
    "retries": 0,
}

with DAG(
    dag_id="setup_kibana_once",
    default_args=default_args,
    description="One-shot : importe le dashboard Kibana après la première indexation ES",
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    tags=["sky_safe", "setup"],
) as dag:

    wait_for_es_data = PythonSensor(
        task_id="wait_for_es_data",
        python_callable=_check_es_index_has_data,
        poke_interval=30,          # vérifie toutes les 30 secondes
        timeout=600,               # abandonne après 10 minutes
        mode="poke",
    )

    import_kibana_dashboard = PythonOperator(
        task_id="import_kibana_dashboard",
        python_callable=importer_dashboard_kibana,
    )

    wait_for_es_data >> import_kibana_dashboard
