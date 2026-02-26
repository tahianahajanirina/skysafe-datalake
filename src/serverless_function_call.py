import requests
import json

def fetch_flights_from_lambda():
    # 1. L'adresse de ton guichet AWS (Colle l'URL générée à l'Étape 1)
    lambda_url = "https://openskynetworkcallbp0mjxji-opensky-network-call.functions.fnc.fr-par.scw.cloud/"
    
    # 2. Les paramètres optionnels que tu veux envoyer à ta Lambda (ex: la bounding box)
    payload = {
        "bounding_box": [41.3, 51.1, -5.1, 9.6]
    }
    
    print(f"Appel de la Lambda via : {lambda_url}")
    
    try:
        # 3. L'envoi du camion de livraison (Méthode POST car on envoie un payload)
        # On met un timeout long (60s) au cas où OpenSky met du temps à répondre au Japon
        response = requests.post(lambda_url, json=payload, timeout=60)
        
        # 4. On vérifie si la Lambda a renvoyé un code 200 (Succès)
        response.raise_for_status()
        
        # 5. On déballe le colis JSON
        data = response.json()
        print(f"Succès ! {data.get('count')} vols récupérés.")
        
        # Affiche un extrait pour prouver que ça marche
        print(json.dumps(data, indent=2)[:500] + "\n... [Suite des données coupée pour l'affichage]")
        
        return data

    except requests.exceptions.HTTPError as err:
        # Si la Lambda a renvoyé une erreur (ex: le code 504 de Timeout)
        print(f"Erreur de l'API Lambda : {err}")
        print(f"Détails : {response.text}")
    except Exception as err:
        print(f"Erreur réseau (Le camion n'a pas pu atteindre AWS) : {err}")
