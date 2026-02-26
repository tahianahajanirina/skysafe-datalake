# Sky-Safe : Construire un Data Lake temps-réel pour l'analyse des risques météorologiques dans le trafic aérien

> **Projet réalisé dans le cadre du cours DATA705 — BDD NoSQL à Télécom Paris**
> Par Tahiana Hajanirina Andriambahoaka, Mohamed Amar et Lounis Hamroun

---

## Table des matières

1. [Introduction et problématique métier](#1-introduction-et-problématique-métier)
2. [Architecture globale du pipeline](#2-architecture-globale-du-pipeline)
3. [Stack technique](#3-stack-technique)
4. [Sources de données — Les APIs](#4-sources-de-données--les-apis)
5. [Organisation du Data Lake — Architecture Medallion](#5-organisation-du-data-lake--architecture-medallion)
6. [Le pipeline étape par étape](#6-le-pipeline-étape-par-étape)
   - 6.1 [Extraction des données de vols (OpenSky via Lambda)](#61-extraction-des-données-de-vols-opensky-via-lambda)
   - 6.2 [Extraction des données météo (Open-Meteo)](#62-extraction-des-données-météo-open-meteo)
   - 6.3 [Formatage des vols avec Spark](#63-formatage-des-vols-avec-spark)
   - 6.4 [Formatage de la météo avec Spark](#64-formatage-de-la-météo-avec-spark)
   - 6.5 [Croisement spatial et Score de Risque](#65-croisement-spatial-et-score-de-risque)
   - 6.6 [Machine Learning — Classification des phases de vol et détection d'anomalies](#66-machine-learning--classification-des-phases-de-vol-et-détection-danomalies)
   - 6.7 [Indexation dans Elasticsearch](#67-indexation-dans-elasticsearch)
7. [Orchestration avec Apache Airflow](#7-orchestration-avec-apache-airflow)
8. [Visualisation avec Kibana](#8-visualisation-avec-kibana)
9. [Infrastructure Docker](#9-infrastructure-docker)
10. [Sécurité — Gestion des secrets](#10-sécurité--gestion-des-secrets)
11. [Stockage Cloud — Amazon S3](#11-stockage-cloud--amazon-s3)
12. [Serverless — Appel de fonction Lambda](#12-serverless--appel-de-fonction-lambda)
13. [Déploiement sur le Cloud — Problèmes rencontrés](#13-déploiement-sur-le-cloud--problèmes-rencontrés)

---

## 1. Introduction et problématique métier

**Comment identifier automatiquement les avions commerciaux qui traversent des zones de turbulences intenses ou d'orages, et détecter les comportements de vol anormaux en temps réel ?**

C'est la question à laquelle Sky-Safe répond. Le projet ingère en quasi temps-réel les positions GPS de milliers d'avions en vol au-dessus de la France, les croise avec les conditions météorologiques locales (vent, pluie, orages, visibilité…), puis calcule un **Score de Risque** pour chaque aéronef. En parallèle, un modèle de **Machine Learning** (K-Means) classifie automatiquement la phase de vol de chaque avion (décollage, croisière, montée/descente) et détecte les **anomalies comportementales** — des avions dont la cinématique s'écarte significativement du trafic global. Le tout est exposé sur un tableau de bord interactif Kibana, permettant aux opérateurs de visualiser d'un coup d'œil les zones dangereuses et les vols suspects.

Le pipeline s'exécute **toutes les minutes**, garantissant une vision quasi temps-réel de la situation aérienne.

---

## 2. Architecture globale du pipeline

Le flux de données suit une architecture **ETL (Extract → Transform → Load)** orchestrée par Airflow et structurée selon le modèle **Medallion** (Raw → Formatted → Enriched → Usage) :

```
  extract_flights ──► format_flights_spark ──┐
                                              ├──► combine_data_spark ──► index_to_elastic
  extract_weather ──► format_weather_spark ──┘       (jointure + ML)
```

**Les deux branches d'extraction s'exécutent en parallèle**, ce qui réduit le temps total d'une exécution du pipeline. La jointure spatiale et le traitement Machine Learning (combine) ne démarrent qu'une fois les deux branches de formatage terminées avec succès. Le ML s'intègre naturellement dans l'étape de combinaison : après la jointure vols × météo et le calcul du score de risque, Spark entraîne un modèle K-Means pour classer les phases de vol et détecter les anomalies, sans ajouter de tâche supplémentaire au DAG.

---

## 3. Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| **Orchestration** | Apache Airflow 2.7 | Planification et exécution automatique du pipeline |
| **Extraction** | Python (Requests) | Appels HTTP vers les APIs OpenSky et Open-Meteo |
| **Transformation** | Apache Spark (PySpark) | Nettoyage, typage, jointure spatiale, calcul du score |
| **Machine Learning** | Spark MLlib (K-Means, StandardScaler) | Classification des phases de vol et détection d'anomalies |
| **Stockage temporaire** | Amazon S3 | Data Lake cloud (architecture Medallion) |
| **Base de données finale** | Elasticsearch 8.10 | Indexation et recherche des documents enrichis |
| **Visualisation** | Kibana 8.10 | Dashboard interactif avec carte et graphiques |
| **Infrastructure** | Docker & Docker Compose | Conteneurisation de tous les services |
| **Serverless** | Scaleway Functions (Lambda) | Proxy pour contourner les limites de l'API OpenSky |

---

## 4. Sources de données — Les APIs

### 4.1 OpenSky Network (données de vols)

L'API [OpenSky Network](https://openskynetwork.github.io/opensky-api/) fournit les positions en temps réel de tous les aéronefs équipés de transpondeurs ADS-B. Pour chaque avion, on obtient :

- **ICAO24** : identifiant unique du transpondeur
- **Callsign** : indicatif d'appel du vol (ex : `AFR1234`)
- **Latitude / Longitude** : position GPS
- **Altitude barométrique et géométrique**
- **Vitesse, cap, taux de montée/descente**
- **Pays d'origine, statut au sol**

> **Problème rencontré** : l'API OpenSky impose des limites de débit strictes. Pour contourner cette contrainte, nous avons déployé une **fonction serverless (Lambda)** sur Scaleway qui sert de proxy et filtre les vols sur une bounding box couvrant la France métropolitaine (`[41.3, 51.1, -5.1, 9.6]`).

### 4.2 Open-Meteo (données météo)

L'API [Open-Meteo](https://open-meteo.com/en/docs) fournit gratuitement des données météorologiques de haute précision. Nous interrogeons les conditions **actuelles** pour 6 aéroports français majeurs :

| Aéroport | Latitude | Longitude |
|---|---|---|
| Paris CDG | 48.71 | 2.21 |
| Toulouse | 43.63 | 1.37 |
| Lyon | 45.73 | 5.09 |
| Marseille | 43.43 | 5.21 |
| Nantes | 47.46 | -0.53 |
| Lille | 50.56 | 3.09 |

Les variables météo récupérées : **température, humidité relative, vitesse et direction du vent, rafales, précipitations, pluie, couverture nuageuse, code météo, visibilité**.

---

## 5. Organisation du Data Lake — Architecture Medallion

Toutes les données transitent par Amazon S3, organisées selon l'architecture **Medallion** (Bronze / Silver / Gold), que nous nommons ici **Raw / Formatted / Enriched / Usage** :

```
s3://data705-opensky-datalake/
├── raw/                          ← Données brutes (JSON)
│   ├── opensky/flights/date=YYYY-MM-DD/hour=HH/
│   └── open_meteo/weather/date=YYYY-MM-DD/hour=HH/
├── formatted/                    ← Données nettoyées (Parquet)
│   ├── opensky/flights/date=YYYY-MM-DD/hour=HH/
│   └── open_meteo/weather/date=YYYY-MM-DD/hour=HH/
├── enriched/                     ← Jointure vols × météo (Parquet)
│   └── sky_safe/flights_weather/date=YYYY-MM-DD/hour=HH/
└── usage/                        ← Prêt pour Elasticsearch (Parquet)
    └── sky_safe/dashboard/date=YYYY-MM-DD/hour=HH/
```

**Pourquoi ce partitionnement `date=/hour=/` ?**
- Permet de retrouver rapidement la dernière partition via l'API S3 (`list_objects_v2` avec `Delimiter`)
- Facilite la rétention et le nettoyage des anciennes données
- Chaque exécution du pipeline écrit dans sa propre partition, sans écraser les précédentes

---

## 6. Le pipeline étape par étape

### 6.1 Extraction des données de vols (OpenSky via Lambda)

**Fichier** : `src/extract_flights.py`

L'extraction des vols ne passe plus directement par l'API OpenSky (limites de débit trop contraignantes), mais par une **fonction serverless** hébergée sur Scaleway Cloud :

1. Appel POST vers la Lambda avec la bounding box de la France en payload
2. La Lambda interroge OpenSky et renvoie les données filtrées
3. Ajout d'un champ `_extracted_at` (horodatage de l'extraction)
4. Sauvegarde JSON brut sur S3 dans `raw/opensky/flights/date=.../hour=.../flights_raw.json`

L'authentification OAuth2 est conservée dans le code pour l'appel direct à OpenSky (fallback), les credentials étant chargés depuis les variables d'environnement.

### 6.2 Extraction des données météo (Open-Meteo)

**Fichier** : `src/extract_weather.py`

Pour chaque point de la grille (6 aéroports), un appel GET est effectué vers l'API Open-Meteo avec les variables météo souhaitées en paramètre. Les résultats sont agrégés dans une liste JSON et sauvegardés sur S3 dans `raw/open_meteo/weather/`.

L'API Open-Meteo est **gratuite et sans authentification**, ce qui simplifie considérablement l'intégration.

### 6.3 Formatage des vols avec Spark

**Fichier** : `src/format_flights.py`

Le JSON brut d'OpenSky contient un tableau `states` où chaque vol est représenté par un **array positionnel** (index 0 = icao24, index 5 = longitude, etc.). Le job Spark :

1. Lit le JSON brut depuis la dernière partition S3
2. Mappe chaque array en un dictionnaire nommé avec typage explicite
3. Nettoie les callsigns (trim, suppression des valeurs vides)
4. Filtre les vols sans coordonnées GPS (`latitude IS NOT NULL AND longitude IS NOT NULL`)
5. Convertit les timestamps epoch en type `timestamp` PySpark
6. Ajoute un label lisible pour la source de position (`ADS-B`, `ASTERIX`, `MLAT`, `FLARM`)
7. Écrit le résultat en **Parquet** dans `formatted/opensky/flights/`

### 6.4 Formatage de la météo avec Spark

**Fichier** : `src/format_weather.py`

Job plus simple : extraction des champs depuis la structure `current` de chaque point géographique, aplatissement en records et écriture Parquet dans `formatted/open_meteo/weather/`.

### 6.5 Croisement spatial et Score de Risque

**Fichier** : `src/combine_spark.py`

C'est le cœur du pipeline. Ce job Spark réalise une **jointure spatiale** entre les vols et les points météo :

#### Étape 1 : Cross Join + Haversine

Chaque vol est croisé avec les 6 points météo. La distance entre le vol et chaque station est calculée via la **formule de Haversine** implémentée en expressions Spark natives (pas de UDF Python, pour des performances optimales) :

$$d = 2R \cdot \arctan2\left(\sqrt{a}, \sqrt{1-a}\right)$$

où $a = \sin^2\!\left(\frac{\Delta\varphi}{2}\right) + \cos(\varphi_1) \cdot \cos(\varphi_2) \cdot \sin^2\!\left(\frac{\Delta\lambda}{2}\right)$

#### Étape 2 : Nearest Weather

Pour chaque vol (`icao24`), seul le **point météo le plus proche** est conservé via une fenêtre Spark (`Window.partitionBy("icao24").orderBy(dist_km.asc())`).

#### Étape 3 : Calcul du Score de Risque (0–100)

Un score composite est calculé selon les règles métier suivantes :

| Condition | Points |
|---|---|
| Orage (`weather_code >= 95`) | +40 |
| Rafales > 80 km/h | +25 |
| Rafales > 50 km/h | +10 |
| Précipitations > 5 mm | +20 |
| Précipitations > 0 mm | +10 |
| Visibilité < 1 000 m | +20 |
| Visibilité < 3 000 m | +10 |
| Couverture nuageuse > 80% | +10 |
| Couverture nuageuse > 50% | +5 |
| Altitude baro < 300 m (en vol) | +15 |

#### Étape 4 : Catégorisation

Le score est traduit en catégorie :
- **HIGH** : score ≥ 60
- **MEDIUM** : score ≥ 30
- **LOW** : score < 30

Le résultat enrichi du score de risque est ensuite transmis à l'étape Machine Learning (section suivante) avant écriture finale en Parquet.

### 6.6 Machine Learning — Classification des phases de vol et détection d'anomalies

**Fichier** : `src/combine_spark.py` (intégré dans le même job Spark que le croisement spatial)

L'API OpenSky ne fournit aucune information sur la **phase de vol** d'un avion (est-il en train de décoller, de monter, de croiser ou d'atterrir ?). De même, elle ne signale pas les comportements anormaux. Sky-Safe utilise le Machine Learning pour déduire automatiquement ces informations à partir des données brutes de cinématique.

#### Approche hybride : K-Means + fallback par règles métier

Le modèle fonctionne selon une approche **hybride** qui s'adapte à la distribution réelle du trafic à l'instant T :

1. **Entraînement K-Means** : à chaque exécution du pipeline, un modèle K-Means (k=3) est entraîné sur les vecteurs normalisés `(velocity, baro_altitude, vertical_rate)` de tous les avions en vol
2. **Vérification de qualité** : la distance maximale entre les centroïdes des 3 clusters est calculée. Si elle dépasse le seuil `MIN_CENTROID_SEPARATION = 1.0` (en espace normalisé), les clusters sont considérés comme significatifs
3. **Deux modes de classification** :

| Mode | Condition | Logique |
|---|---|---|
| **K-Means** | Clusters bien séparés (distance max ≥ 1.0) | Labellisation automatique par altitude moyenne croissante : cluster le plus bas → *Takeoff / Landing*, intermédiaire → *Climb / Descent*, le plus haut → *Cruise* |
| **Règles métier** (fallback) | Clusters trop proches (distance max < 1.0) — ex. la nuit, quasi tous les avions sont en croisière | Seuils aéronautiques fixes : altitude < 300 m + vitesse < 60 m/s → *Takeoff / Landing*, altitude > 3 000 m + taux vertical quasi nul → *Cruise*, sinon → *Climb / Descent* |

**Pourquoi cette approche hybride ?** Le K-Means force toujours k clusters, même si les données sont homogènes. À 3 h du matin, quand tous les avions survolant la France sont des long-courriers en croisière stable, le K-Means découperait artificiellement ce groupe en 3 sous-groupes sans signification. Le mécanisme de vérification détecte cette situation et bascule sur des règles métier fiables.

#### Pipeline ML Spark

```python
# VectorAssembler → StandardScaler → KMeans
assembler = VectorAssembler(inputCols=["velocity", "baro_altitude", "vertical_rate"], outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
kmeans = KMeans(featuresCol="scaled_features", k=3, seed=42, maxIter=20)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(df)
df_clustered = model.transform(df)
```

La normalisation via `StandardScaler` est essentielle : sans elle, l'altitude (valeurs en milliers de mètres) dominerait complètement la vitesse et le taux vertical dans le calcul des distances.

#### Détection d'anomalies par distance au centroïde

Une fois les clusters formés, chaque avion possède un centroïde de référence (le centre de son cluster). La **distance euclidienne** entre l'avion et son centroïde (en espace normalisé) mesure à quel point cet avion s'écarte du comportement typique de son groupe :

$$\text{anomaly\_score} = \sqrt{(v - c_v)^2 + (a - c_a)^2 + (r - c_r)^2}$$

où $(v, a, r)$ sont les features normalisées du vol et $(c_v, c_a, c_r)$ les coordonnées du centroïde.

Le seuil d'anomalie est calculé dynamiquement sur la distribution des distances :

$$\text{seuil} = \mu_d + 2 \times \sigma_d$$

Tout vol dont l'`anomaly_score` dépasse ce seuil est marqué `is_anomaly = True`. En statistique, cela correspond environ aux **5 % de vols les plus atypiques** dans la distribution.

**Exemple concret** : un avion classé « Cruise » (haute altitude) mais qui vole anormalement lentement avec un fort taux de descente sera très éloigné du centroïde de croisière → anomalie détectée. Cela peut indiquer un problème mécanique, une manœuvre d'urgence, ou un déroutement.

#### Colonnes produites par le ML

| Colonne | Type | Description |
|---|---|---|
| `flight_phase` | keyword | Phase de vol déduite : *Takeoff / Landing*, *Climb / Descent* ou *Cruise* |
| `flight_phase_id` | integer | Identifiant numérique du cluster (0, 1 ou 2) |
| `is_anomaly` | boolean | `true` si le vol est détecté comme anomalie |
| `anomaly_score` | float | Distance au centroïde — plus c'est élevé, plus le comportement est atypique |

Le résultat complet (score de risque + ML) est écrit en Parquet dans `enriched/sky_safe/flights_weather/`.

### 6.7 Indexation dans Elasticsearch

**Fichier** : `src/index_elastic.py`

Ce module opère en deux phases :

**Phase 1 — Couche Usage** : lecture du Parquet enrichi, sélection et renommage des colonnes pertinentes pour le dashboard, écriture Parquet dans `usage/sky_safe/dashboard/`.

**Phase 2 — Bulk Insert** :
1. Lecture de la couche Usage
2. Conversion de chaque row en document ES : fusion `latitude`/`longitude` en un champ `location` de type `geo_point` (requis par Kibana Maps)
3. Création de l'index avec un mapping explicite si inexistant (types `keyword`, `float`, `geo_point`, `date`, `boolean`…). Le mapping inclut les champs ML : `flight_phase` (keyword), `flight_phase_id` (integer), `is_anomaly` (boolean) et `anomaly_score` (float)
4. Indexation en masse via l'API `bulk` d'Elasticsearch

L'identifiant `_id` de chaque document est l'`icao24` de l'avion, ce qui fait qu'un avion est **mis à jour à chaque exécution** plutôt que dupliqué.

---

## 7. Orchestration avec Apache Airflow

### Le DAG principal : `sky_safe_pipeline`

**Fichier** : `dags/sky_safe_dag.py`

Le DAG s'exécute **toutes les minutes** (`schedule='* * * * *'`) et enchaîne 6 tâches :

```
extract_flights_api  ──►  format_flights_spark  ──┐
                                                    ├──►  combine_data_spark  ──►  index_to_elastic
extract_weather_api  ──►  format_weather_spark  ──┘
```

Propriétés clés :
- `catchup=False` : pas de rattrapage des exécutions passées
- `max_active_runs=1` : une seule exécution à la fois (évite les conflits de données)
- `trigger_rule='all_success'` sur le combine : la jointure ne s'exécute que si les deux formatages réussissent
- `retries=1` avec `retry_delay=1 min` : un seul retry automatique en cas d'échec

### Le DAG de setup : `setup_kibana_once`

**Fichier** : `dags/setup_kibana_dag.py`

Un DAG `@once` (exécution unique) qui :
1. **Attend** que l'index Elasticsearch contienne au moins un document (via un `PythonSensor` qui poll toutes les 30 secondes, timeout 10 min)
2. **Importe** automatiquement le dashboard Kibana pré-configuré via l'API Saved Objects

Cela permet une mise en place 100% automatique du dashboard dès la première exécution du pipeline.

---

## 8. Visualisation avec Kibana

Le dashboard Kibana est importé automatiquement depuis un fichier NDJSON (`src/dashboard/kibana_dashboard_config.ndjson`) et offre une vue interactive sur :

- **Carte géographique** : position de chaque avion avec code couleur selon le `risk_category` ou la `flight_phase` (phase de vol déduite par le ML)
- **Carte des anomalies** : les avions normaux apparaissent en vert, les anomalies détectées par le modèle ML en rouge vif — un contrôleur aérien identifie immédiatement les vols suspects
- **Distribution des scores de risque** : histogramme et statistiques
- **Distribution des phases de vol** : répartition circulaire (pie chart) *Takeoff / Landing*, *Climb / Descent*, *Cruise* — permet de voir d'un coup d'œil la composition du trafic
- **Histogramme des anomaly scores** : visualise la distribution des distances au centroïde et le seuil au-delà duquel un vol est considéré comme atypique
- **Conditions météo en temps réel** : vent, visibilité, précipitations par zone
- **Tableau détaillé** : callsign, pays, altitude, vitesse, score, catégorie, phase de vol, anomalie

Le champ `location` (type `geo_point`) est la clé technique qui permet l'affichage cartographique dans Kibana Maps.

---

## 9. Infrastructure Docker

L'ensemble du projet est conteneurisé via **Docker Compose** (5 services) :

| Service | Image | Rôle |
|---|---|---|
| `postgres` | `postgres:13` | Base de données interne d'Airflow (métadonnées, états des DAGs) |
| `airflow-init` | Custom (basée sur `apache/airflow:2.7.1`) | Migration BDD + création de l'utilisateur admin |
| `airflow-webserver` | Custom | Interface web Airflow (port 8090) |
| `airflow-scheduler` | Custom | Planificateur qui déclenche le DAG toutes les minutes |
| `elasticsearch` | `elasticsearch:8.10.2` | Moteur de recherche et base de données finale |
| `kibana` | `kibana:8.10.2` | Dashboard de visualisation (port 5601) |

### Le Dockerfile

```dockerfile
FROM apache/airflow:2.7.1
# Installation de Java 11 (requis par PySpark)
# Téléchargement des JARs Hadoop-AWS pour le support S3A dans Spark
# Installation des dépendances Python
```

Points notables :
- **Java 11** est installé pour PySpark
- Les **JARs Hadoop AWS** (`hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar`) sont téléchargés pour permettre à Spark de lire/écrire directement sur S3 via le protocole `s3a://`
- Les volumes Docker montent `dags/`, `src/` et `data/` en temps réel : **toute modification du code est appliquée sans rebuild**

### Healthchecks et dépendances

L'orchestration des démarrages est gérée par des **healthchecks** et des conditions `depends_on` :
- PostgreSQL doit être healthy avant tout service Airflow
- `airflow-init` doit se terminer avec succès avant le webserver et le scheduler
- Elasticsearch doit être healthy avant le webserver (pour que le DAG puisse indexer)
- Kibana attend Elasticsearch

---

## 10. Sécurité — Gestion des secrets

**Aucun secret n'est exposé en dur dans le code source.** Toutes les données sensibles sont externalisées dans un fichier `.env` (ignoré par Git via `.gitignore`) :

| Variable | Description |
|---|---|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | Credentials de la base PostgreSQL |
| `AIRFLOW__CORE__FERNET_KEY` | Clé de chiffrement des connexions Airflow |
| `AIRFLOW__CORE__SECRET_KEY` | Clé JWT partagée webserver/scheduler |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Identifiants de l'utilisateur Airflow |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credentials AWS pour l'accès S3 |
| `SKY_NETWORK_CLIENT_ID` / `SKY_NETWORK_CLIENT_SECRET` | Credentials OAuth2 OpenSky Network |

Un fichier `.env.example` est fourni comme template, avec des valeurs `CHANGE_ME` à remplacer.

---

## 11. Stockage Cloud — Amazon S3

Le Data Lake est hébergé sur **Amazon S3** (bucket `data705-opensky-datalake`, région `eu-north-1`). Le module `helpers.py` centralise toutes les interactions S3 :

- **`save_json()`** : sérialise un objet Python en JSON et l'écrit sur S3 via `boto3.put_object()`
- **`read_json()`** : lit un fichier JSON depuis S3 via `boto3.get_object()`
- **`latest_partition()`** : parcourt les préfixes S3 (`date=` puis `hour=`) pour trouver automatiquement la partition la plus récente
- **`get_spark()`** : configure la SparkSession avec les credentials S3 et le endpoint `s3a://` pour que Spark lise et écrive directement sur S3

---

## 12. Serverless — Appel de fonction Lambda

**Fichier** : `src/serverless_function_call.py`

Pour contourner les limitations de débit de l'API OpenSky, une **fonction serverless** Scaleway est déployée. Le code Python envoie un POST avec la bounding box de la France :

```python
payload = {"bounding_box": [41.3, 51.1, -5.1, 9.6]}
response = requests.post(lambda_url, json=payload, timeout=60)
```

La Lambda agit comme un **proxy intelligent** : elle interroge OpenSky et renvoie les données filtrées par zone géographique, réduisant la charge et le volume de données transitant vers notre pipeline.

---

## 13. Déploiement sur le Cloud — Problèmes rencontrés

> *Section à compléter avec les retours d'expérience du déploiement.*

<!-- 
TEMPLATE — Remplacer par vos véritables retours d'expérience :

### 13.1 Problème : [Nom du problème]
**Contexte** : ...
**Erreur rencontrée** : ...
**Solution** : ...

### 13.2 Problème : [Nom du problème]
**Contexte** : ...
**Erreur rencontrée** : ...
**Solution** : ...
-->

---

## Conclusion

Sky-Safe démontre qu'il est possible de construire un pipeline Big Data **complet, automatisé et temps-réel** avec des outils open source et des APIs gratuites. L'architecture Medallion garantit la traçabilité des données à chaque étape, Spark assure les transformations lourdes, et Elasticsearch + Kibana offrent une visualisation interactive immédiate.

Les choix techniques clés qui ont fait la différence :
- **Parallélisme des extractions** dans le DAG Airflow (les deux APIs sont interrogées simultanément)
- **Formule de Haversine en Spark natif** (pas de UDF Python coûteuse)
- **Machine Learning hybride** : K-Means pour la classification des phases de vol quand le trafic est hétérogène, fallback automatique sur des règles métier aéronautiques quand les données sont homogènes (évite les faux clusters)
- **Détection d'anomalies sans supervision** : la distance euclidienne au centroïde K-Means, combinée à un seuil statistique dynamique ($\mu + 2\sigma$), identifie les vols au comportement atypique sans avoir besoin de données labellisées
- **Partitionnement temporel sur S3** (retrouver la dernière partition en O(1) via l'API S3)
- **Indexation par `icao24`** dans Elasticsearch (upsert naturel, pas de doublons)
- **Setup automatique de Kibana** via un DAG sensor (zéro intervention manuelle)

---

*Projet Sky-Safe — DATA705 BDD NoSQL — Télécom Paris — 2026*
