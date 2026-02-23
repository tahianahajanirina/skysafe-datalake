# ✈️ Sky-Safe : Flight & Weather Risk Analytics Pipeline

## 📖 Description du Projet
**Sky-Safe** est une plateforme de traitement de données Big Data conçue pour analyser les risques météorologiques liés au trafic aérien en quasi temps-réel. 

La problématique métier que nous résolvons est la suivante : *Comment identifier automatiquement les avions commerciaux qui s'apprêtent à traverser des zones de turbulences intenses ou d'orages, afin d'optimiser la sécurité des vols ?*

Pour y répondre, notre pipeline ingère, nettoie et croise les données de géolocalisation des vols en direct avec les prévisions météorologiques locales, avant d'exposer un "Score de Risque" sur un tableau de bord interactif.

## 🛠️ Stack Technique
* **Orchestration :** Apache Airflow (DAG déclenché toutes les 2 minutes)
* **Ingestion (Extract) :** Python (Requests, Pandas)
* **Transformation (Format & Combine) :** Apache Spark (PySpark)
* **Base de données & Recherche :** Elasticsearch
* **Visualisation :** Kibana
* **Infrastructure :** Docker & Docker Compose
* **Stockage :** Data Lake local organisé selon l'architecture Medallion

## 📡 Sources de Données (APIs Open Source)
1. **OpenSky Network API** (https://openskynetwork.github.io/opensky-api/) : Suivi des vols en temps réel (Latitude, Longitude, Altitude, ICAO24).
2. **Open-Meteo API** (https://open-meteo.com/en/docs) : Données météorologiques haute précision (Vitesse du vent, Précipitations, Orages) basées sur un système de grille géographique.

## 📂 Architecture du Data Lake (Clean Naming)
Le projet respecte une hiérarchie stricte de stockage des fichiers pour garantir la traçabilité de la donnée : `data/<layer>/<group>/<dataEntity>/date=YYYY-MM-DD/hour=HH/`

* `data/raw/` : Données JSON brutes fraîchement extraites des APIs.
* `data/formatted/` : Données nettoyées, typées et converties au format optimisé Parquet via Spark.
* `data/enriched/` : Données massives issues de la jointure spatiale entre les avions et la météo (Spark Join).
* `data/usage/` : Données agrégées et allégées, prêtes à être ingérées par Elasticsearch.

## 🚀 Installation et Lancement

### Prérequis
* Docker et Docker Compose installés sur votre machine.

### Étape 1 : Démarrer l'infrastructure
Placez-vous à la racine du projet et montez les conteneurs Docker (Airflow, Postgres, Elasticsearch, Kibana) :
`docker-compose up -d --build`

### Étape 2 : Accéder aux interfaces
Une fois les conteneurs démarrés, les services sont accessibles aux adresses suivantes :
* **Apache Airflow :** http://localhost:8080 (Identifiants : admin / admin)
* **Kibana (Dashboard) :** http://localhost:5601
* **Elasticsearch :** http://localhost:9200

### Étape 3 : Activer le Pipeline
1. Connectez-vous à l'interface Airflow.
2. Localisez le DAG nommé `sky_safe_pipeline`.
3. Activez le bouton "Unpause" (le toggle switch) pour lancer l'exécution automatisée toutes les 2 minutes.

---

## 🎛️ Manuel de Pilotage au Quotidien

La commande `docker-compose up -d --build` a construit l'image Docker, branché l'infrastructure et démarré tous les services. Voici les 4 situations que vous rencontrerez au quotidien.

### 1. Juste après le premier `up -d --build` — La Vérification

L'usine tourne en arrière-plan (c'est le rôle du `-d` pour *detached*).

1. Ouvrez votre navigateur.
2. Accédez à `http://localhost:8080` (Airflow) et `http://localhost:5601` (Kibana).
3. Si les pages s'affichent, l'infrastructure est opérationnelle.

### 2. Modification du code Python (`src/` ou `dags/`)

> **Aucune action Docker requise.**

Les dossiers `src/`, `dags/` et `data/` sont montés en tant que **volumes Docker** — les conteneurs lisent directement les fichiers de votre machine en temps réel.

1. Modifiez votre code dans l'éditeur.
2. Sauvegardez (`Ctrl + S`).
3. Airflow détectera automatiquement la modification sous ~30 secondes et utilisera le nouveau code à la prochaine exécution.

### 3. Modification du `requirements.txt` — Ajout d'une librairie

> **L'image doit être reconstruite.**

Une nouvelle dépendance implique de rebuilder l'image Airflow. Exécutez à nouveau :

```bash
docker-compose up -d --build
```

Docker est incrémental : il ne recrée que les conteneurs impactés (Airflow), sans toucher à Elasticsearch, Kibana ou la base de données.

### 4. Fin de journée / Reprise le lendemain

**Le soir — Éteindre l'infrastructure :**
```bash
docker-compose stop
```
Les conteneurs s'arrêtent proprement. Les données dans `./data` et les configurations Airflow sont conservées.

**Le lendemain — Rallumer l'infrastructure :**
```bash
docker-compose start
```
L'infrastructure repart exactement dans l'état où elle a été arrêtée. Pas besoin de rebuild.

### 5. Suppression complète des conteneurs et volumes

> **⚠️ Opération destructive** — toutes les données stockées dans les volumes (base de données Airflow, index Elasticsearch) seront effacées.

```bash
docker-compose down -v
```

À utiliser uniquement pour repartir de zéro (reset complet) ou nettoyer l'environnement en fin de projet.

---

> **Résumé :** Au quotidien → `start` pour allumer, `stop` pour éteindre, code normalement sur votre machine. Ne relancez `--build` que lors de l'ajout d'une nouvelle librairie dans `requirements.txt`. Utilisez `down -v` uniquement pour un reset complet.

---

## 👥 L'Équipe

Projet réalisé dans le cadre du cours **DATA705 - BDD NoSQL** à [Télécom Paris](https://www.telecom-paris.fr/).

| Nom | GitHub |
|-----|--------|
| Tahiana Hajanirina Andriambahoaka | [@tahianahajanirina](https://github.com/tahianahajanirina) |
| Mohammed Ammar | [@mohammed-ammar](https://github.com/mohammed-ammar) |
| Lounis | [@lounis](https://github.com/lounis) |