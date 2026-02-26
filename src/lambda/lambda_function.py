import json
import os
import socket
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

def lambda_handler(event, context):
    """
    Point d'entrée AWS Lambda.
    Retourne toujours une réponse formatée en JSON strict.
    """
    # L'étiquette magique pour dire à Internet "Ceci est du JSON"
    json_headers = {
        "Content-Type": "application/json"
    }

    try:
        # ── Configuration ─────────────────────────────────────────────────
        base_url      = os.environ.get("SKY_NETWORK_BASE_URL", "https://opensky-network.org/api")
        token_url     = ("https://auth.opensky-network.org/auth/realms/opensky-network"
                         "/protocol/openid-connect/token")
        client_id     = os.environ.get("SKY_NETWORK_CLIENT_ID", "tahiana-api-client")
        client_secret = os.environ.get("SKY_NETWORK_CLIENT_SECRET", "OfiQ7YzZIJyBLAaqKHYb0BMx3LXXKKwK")

        headers_base = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        # ── 0. HEALTH CHECK (Test de l'IP) ────────────────────────────────
        print("Étape 0 : Vérification rapide du statut de l'IP...")
        try:
            test_req = Request(token_url, headers=headers_base, method="HEAD")
            urlopen(test_req, timeout=5)
            print("✅ DIAGNOSTIC : L'IP est OK !")
        except HTTPError as e:
            print(f"✅ DIAGNOSTIC : L'IP est OK ! (Le serveur répond, code {e.code}).")
        except (URLError, socket.timeout, TimeoutError) as e:
            print("❌ DIAGNOSTIC : L'IP EST BANNIE (Timeout). Arrêt immédiat.")
            return {
                "statusCode": 504,
                "headers": json_headers,
                "body": json.dumps({
                    "error": "Échec au diagnostic initial : l'IP est bloquée.",
                    "details": str(e)
                })
            }

        # ── Validation du bounding_box ────────────────────────────────────
        bounding_box = event.get("bounding_box")
        if bounding_box is not None:
            if not isinstance(bounding_box, (list, tuple)) or len(bounding_box) != 4:
                return {
                    "statusCode": 400,
                    "headers": json_headers,
                    "body": json.dumps({
                        "error": "bounding_box invalide. Doit être une liste de 4 valeurs.",
                        "example": [41.3, 51.1, -5.1, 9.6]
                    })
                }

        # ── 1. Authentification OAuth2 ────────────────────────────────────
        print("Étape 1 : Demande de Token en cours...")
        body = urlencode({
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }).encode("utf-8")
        
        token_headers_req = headers_base.copy()
        token_headers_req["Content-Type"] = "application/x-www-form-urlencoded"
        
        req_token = Request(token_url, data=body, headers=token_headers_req, method="POST")
        with urlopen(req_token, timeout=15) as resp:
            token = json.loads(resp.read())["access_token"]
        print("Étape 1 : Token récupéré avec succès !")

        # ── 2. Appel API /states/all ──────────────────────────────────────
        print("Étape 2 : Extraction des vols en cours...")
        params = {}
        if bounding_box:
            lat_min, lat_max, lon_min, lon_max = bounding_box
            params = {"lamin": lat_min, "lamax": lat_max, "lomin": lon_min, "lomax": lon_max}

        url = f"{base_url}/states/all"
        if params:
            url += "?" + urlencode(params)

        api_headers = headers_base.copy()
        api_headers["Authorization"] = f"Bearer {token}"
        
        req_api = Request(url, headers=api_headers)
        with urlopen(req_api, timeout=60) as resp:
            data = json.loads(resp.read())
        print("Étape 2 : Vols récupérés avec succès !")

        # ── 3. Enrichissement et réponse finale JSON ──────────────────────
        states = data.get("states") or []
        data["_extracted_at"] = datetime.utcnow().isoformat()
        data["count"] = len(states)

        return {
            "statusCode": 200,
            "headers": json_headers,  # <--- C'est ici que la magie opère pour le rendu JSON
            "body": json.dumps(data)
        }

    # ── GESTION DÉTAILLÉE DES ERREURS (en JSON aussi) ─────────────────
    except HTTPError as e:
        error_message = e.read().decode('utf-8') if e.fp else "Pas de détails"
        return {
            "statusCode": e.code,
            "headers": json_headers,
            "body": json.dumps({
                "error": "Refus de l'API OpenSky",
                "http_code": e.code,
                "reason": e.reason,
                "opensky_message": error_message
            })
        }
        
    except (URLError, socket.timeout, TimeoutError) as e:
        return {
            "statusCode": 504,
            "headers": json_headers,
            "body": json.dumps({
                "error": "Impossible de joindre le serveur. IP potentiellement bannie ou Timeout.",
                "details": str(e)
            })
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": json_headers,
            "body": json.dumps({
                "error": "Erreur interne de la fonction",
                "details": str(e),
                "type": type(e).__name__
            })
        }

# if __name__ == "__main__":
#     # Test local rapide (sans AWS) pour vérifier que la fonction fonctionne
#     test_event = {
#         "bounding_box": [41.3, 51.1, -5.1, 9.6]  # Exemple de bounding box pour la France
#     }
#     response = lambda_handler(test_event, None)
#     print("Réponse de test local :")
#     print(json.dumps(json.loads(response["body"]), indent=2))