import os
import time

import requests

# ─── Configuration ────────────────────────────────────────────────────────────
KIBANA_URL = os.getenv("KIBANA_URL", "http://kibana:5601")
IMPORT_ENDPOINT = f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true"
NDJSON_PATH = "/opt/airflow/src/dashboard/kibana_dashboard.ndjson"
MAX_RETRIES = 12          # Kibana peut mettre du temps à démarrer
RETRY_DELAY_SEC = 10


def wait_for_kibana() -> bool:
    """Attend que Kibana soit prêt avant d'importer."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            if r.status_code == 200:
                print(f"Kibana prêt (tentative {attempt}).")
                return True
        except requests.ConnectionError:
            pass
        print(f"Kibana pas encore prêt, nouvelle tentative dans {RETRY_DELAY_SEC}s... ({attempt}/{MAX_RETRIES})")
        time.sleep(RETRY_DELAY_SEC)
    return False


def importer_dashboard_kibana() -> None:
    # 1. Vérifier que le fichier NDJSON existe
    if not os.path.exists(NDJSON_PATH):
        print(f"Erreur : {NDJSON_PATH} introuvable.")
        print("Exporte ton dashboard depuis Kibana > Stack Management > Saved Objects > Export.")
        return

    # 2. Attendre que Kibana soit opérationnel
    if not wait_for_kibana():
        print("Kibana n'a pas répondu après toutes les tentatives. Abandon.")
        return

    # 3. Import via l'API Saved Objects
    headers = {"kbn-xsrf": "true"}

    print(f"Import de {NDJSON_PATH} vers {IMPORT_ENDPOINT}...")
    with open(NDJSON_PATH, "rb") as f:
        resp = requests.post(IMPORT_ENDPOINT, headers=headers, files={"file": f}, timeout=30)

    # 4. Résultat
    if resp.status_code == 200:
        data = resp.json()
        success = data.get("successCount", 0)
        errors = data.get("errors", [])
        print(f"Import terminé : {success} objet(s) importé(s).")
        if errors:
            for err in errors:
                print(f"  Erreur : {err.get('id')} — {err.get('error', {}).get('message')}")
    else:
        print(f"Échec de l'import (HTTP {resp.status_code}).")
        print(resp.text)


if __name__ == "__main__":
    importer_dashboard_kibana()