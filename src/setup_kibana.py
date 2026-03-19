"""
setup_kibana.py
---------------
Import automatique du dashboard Kibana via l'API Saved Objects.
Attend que Kibana soit opérationnel avant de lancer l'import.
"""

import os
import time

import requests

from helpers import logger

# ─── Configuration ────────────────────────────────────────────────────────────
KIBANA_URL = os.getenv("KIBANA_URL", "http://kibana:5601")
IMPORT_ENDPOINT = f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true"
NDJSON_PATH = "/opt/airflow/src/dashboard/kibana_dashboard_config.ndjson"
MAX_RETRIES = 12          # Kibana peut mettre du temps à démarrer
RETRY_DELAY_SEC = 10


def wait_for_kibana() -> bool:
    """Attend que Kibana soit prêt avant d'importer."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            if r.status_code == 200:
                logger.info("Kibana prêt (tentative %d).", attempt)
                return True
        except requests.ConnectionError:
            pass
        logger.info(
            "Kibana pas encore prêt, nouvelle tentative dans %ds... (%d/%d)",
            RETRY_DELAY_SEC, attempt, MAX_RETRIES,
        )
        time.sleep(RETRY_DELAY_SEC)
    return False


def importer_dashboard_kibana() -> None:
    """Importe le dashboard Kibana depuis le fichier NDJSON."""
    # 1. Vérifier que le fichier NDJSON existe
    if not os.path.exists(NDJSON_PATH):
        logger.error("Fichier introuvable : %s", NDJSON_PATH)
        logger.error("Exporte ton dashboard depuis Kibana > Stack Management > Saved Objects > Export.")
        return

    # 2. Attendre que Kibana soit opérationnel
    if not wait_for_kibana():
        logger.error("Kibana n'a pas répondu après toutes les tentatives. Abandon.")
        return

    # 3. Import via l'API Saved Objects
    headers = {"kbn-xsrf": "true"}

    logger.info("Import de %s vers %s...", NDJSON_PATH, IMPORT_ENDPOINT)
    with open(NDJSON_PATH, "rb") as f:
        resp = requests.post(IMPORT_ENDPOINT, headers=headers, files={"file": f}, timeout=30)

    # 4. Résultat
    if resp.status_code == 200:
        data = resp.json()
        success = data.get("successCount", 0)
        errors = data.get("errors", [])
        logger.info("Import terminé : %d objet(s) importé(s).", success)
        if errors:
            for err in errors:
                logger.error("  Erreur : %s — %s", err.get("id"), err.get("error", {}).get("message"))
    else:
        logger.error("Échec de l'import (HTTP %d).", resp.status_code)
        logger.error("%s", resp.text)


if __name__ == "__main__":
    importer_dashboard_kibana()