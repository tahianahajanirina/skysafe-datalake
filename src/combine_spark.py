"""
combine_spark.py
----------------
Job Spark : formatted → enriched
Jointure spatiale vols × météo + calcul du Score de Risque.
Machine Learning : K-Means (phases de vol) + détection d'anomalies.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

from helpers import get_spark, latest_partition, output_path, logger


# ─────────────────────────────────────────────────────────────────────────────
# MACHINE LEARNING — Classification des phases de vol & détection d'anomalies
# ─────────────────────────────────────────────────────────────────────────────

# Nombre de clusters K-Means (3 phases : décollage/atterrissage, croisière, montée/descente)
ML_K_CLUSTERS = 3
# Seuil de distance au centroïde au-delà duquel un vol est considéré comme anomalie
# (multiplicateur de l'écart-type : tout point > mean + ANOMALY_SIGMA * std = anomalie)
ANOMALY_SIGMA = 2.0
# Distance minimale entre centroïdes (espace normalisé) pour considérer que les
# clusters K-Means sont réellement distincts. En dessous, on bascule sur les règles
# métier aéronautiques pour éviter de classer artificiellement des données homogènes.
MIN_CENTROID_SEPARATION = 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Haversine via expressions Spark natives (pas de UDF Python)
# ─────────────────────────────────────────────────────────────────────────────

def haversine_expr(lat1, lon1, lat2, lon2):
    """
    Retourne une Column Spark = distance en km (formule de Haversine).
    Utilise uniquement des fonctions Spark natives → pas de sérialisation Python.
    """
    R = 6371.0  # rayon moyen de la Terre (km)
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    d_phi = F.radians(lat2 - lat1)
    d_lambda = F.radians(lon2 - lon1)
    a = (
        F.sin(d_phi / 2) ** 2
        + F.cos(phi1) * F.cos(phi2) * F.sin(d_lambda / 2) ** 2
    )
    return F.lit(R) * 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))


# ─────────────────────────────────────────────────────────────────────────────
# JOB COMBINE
# ─────────────────────────────────────────────────────────────────────────────

def combine_data_main(spark=None) -> str:
    """
    Jointure spatiale vols × météo et calcul du Score de Risque.

    Étapes :
      1. Lire les Parquet formatés (flights + weather) via latest_partition().
      2. Cross join vols × points météo, calculer la distance Haversine
         et ne garder que le point météo le plus proche de chaque vol.
      3. Calculer un score de risque (0–100).
      4. Ajouter risk_category : LOW / MEDIUM / HIGH.
      5. Écrire en Parquet dans data/enriched/sky_safe/flights_weather/.

    Returns : chemin du répertoire Parquet écrit.
    """
    logger.info("=== Démarrage combine_data ===")

    # ── 0. Spark ─────────────────────────────────────────────────────────────
    if spark is None:
        spark = get_spark()

    # ── 1. Lecture des Parquet formatés ──────────────────────────────────────
    flights_dir = latest_partition("formatted", "opensky", "flights")
    weather_dir = latest_partition("formatted", "open_meteo", "weather")

    logger.info("Lecture flights depuis : %s", flights_dir)
    logger.info("Lecture weather depuis : %s", weather_dir)

    df_flights = spark.read.parquet(flights_dir)
    df_weather = spark.read.parquet(weather_dir)

    logger.info("Vols chargés : %d | Points météo chargés : %d",
                df_flights.count(), df_weather.count())

    # ── 2. Préfixer les colonnes météo pour éviter les collisions ────────────
    weather_cols_to_prefix = [c for c in df_weather.columns if c not in ("latitude", "longitude")]
    for col_name in weather_cols_to_prefix:
        df_weather = df_weather.withColumnRenamed(col_name, "w_" + col_name)
    df_weather = (
        df_weather
        .withColumnRenamed("latitude", "w_latitude")
        .withColumnRenamed("longitude", "w_longitude")
    )

    # ── 3. Cross join + distance Haversine ───────────────────────────────────
    df_cross = df_flights.crossJoin(df_weather)

    df_cross = df_cross.withColumn(
        "dist_km",
        haversine_expr(
            F.col("latitude"), F.col("longitude"),
            F.col("w_latitude"), F.col("w_longitude"),
        ),
    )

    # ── 4. Garder uniquement le point météo le plus proche par vol ───────────
    window_nearest = Window.partitionBy("icao24").orderBy(F.col("dist_km").asc())

    df_nearest = (
        df_cross
        .withColumn("_rank", F.row_number().over(window_nearest))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    logger.info("Lignes après nearest-weather join : %d", df_nearest.count())

    # ── 5. Calcul du Score de Risque (0–100) ────────────────────────────────
    #   - Orage  (weather_code >= 95)      → +40
    #   - Rafales > 80 km/h               → +25  |  > 50 km/h → +10
    #   - Précipitations > 5 mm           → +20  |  > 0 mm    → +10
    #   - Visibilité < 1 000 m            → +20  |  < 3 000 m → +10
    #   - Couverture nuageuse > 80 %      → +10  |  > 50 %    → +5
    #   - Altitude baro < 300 m (en vol)  → +15

    risk_expr = (
        # Orage
        F.when(F.col("w_weather_code") >= 95, F.lit(40)).otherwise(F.lit(0))

        # Rafales
        + F.when(F.col("w_wind_gusts_10m") > 80, F.lit(25))
           .when(F.col("w_wind_gusts_10m") > 50, F.lit(10))
           .otherwise(F.lit(0))

        # Précipitations
        + F.when(F.col("w_precipitation") > 5, F.lit(20))
           .when(F.col("w_precipitation") > 0, F.lit(10))
           .otherwise(F.lit(0))

        # Visibilité
        + F.when(F.col("w_visibility") < 1000, F.lit(20))
           .when(F.col("w_visibility") < 3000, F.lit(10))
           .otherwise(F.lit(0))

        # Couverture nuageuse
        + F.when(F.col("w_cloud_cover") > 80, F.lit(10))
           .when(F.col("w_cloud_cover") > 50, F.lit(5))
           .otherwise(F.lit(0))

        # Altitude basse + en vol (on_ground == False)
        + F.when(
            (F.col("on_ground") == False) & (F.col("baro_altitude") < 300),  # noqa: E712
            F.lit(15),
          ).otherwise(F.lit(0))
    )

    df_scored = df_nearest.withColumn("risk_score", risk_expr.cast(IntegerType()))

    # ── 6. Catégorisation du risque ──────────────────────────────────────────
    df_scored = df_scored.withColumn(
        "risk_category",
        F.when(F.col("risk_score") >= 60, F.lit("HIGH"))
         .when(F.col("risk_score") >= 30, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW")),
    )

    # ── 7. MACHINE LEARNING — K-Means (phases de vol) + anomalies ────────────
    #
    #   On utilise velocity, baro_altitude et vertical_rate comme features.
    #   Approche HYBRIDE :
    #     - On entraîne K-Means (k=3) pour regrouper les avions
    #     - On vérifie si les clusters sont réellement séparés (check de qualité)
    #     - Si les clusters sont trop proches (données homogènes, ex. tous en
    #       croisière à 3h du matin), on bascule sur une classification par
    #       RÈGLES MÉTIER aéronautiques pour éviter des groupes artificiels
    #     - La détection d'anomalies fonctionne dans les deux cas
    # ─────────────────────────────────────────────────────────────────────────

    logger.info("=== Machine Learning : K-Means + détection d'anomalies ===")

    # Colonnes utilisées comme features pour le modèle ML
    ml_features = ["velocity", "baro_altitude", "vertical_rate"]

    # Remplacer les nulls par 0 pour les features ML (évite les erreurs VectorAssembler)
    df_ml = df_scored
    for feat in ml_features:
        df_ml = df_ml.withColumn(feat, F.coalesce(F.col(feat), F.lit(0.0)))

    # Pipeline ML : VectorAssembler → StandardScaler → KMeans
    assembler = VectorAssembler(inputCols=ml_features, outputCol="_ml_features_raw")
    scaler = StandardScaler(
        inputCol="_ml_features_raw",
        outputCol="_ml_features",
        withStd=True,
        withMean=True,
    )
    kmeans = KMeans(
        featuresCol="_ml_features",
        predictionCol="_ml_cluster_id",
        k=ML_K_CLUSTERS,
        seed=42,
        maxIter=20,
    )

    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(df_ml)
    df_clustered = model.transform(df_ml)

    # ── 7a. Vérification de la qualité des clusters ─────────────────────────
    #   On mesure la séparation entre centroïdes en espace normalisé.
    #   Si la distance max entre deux centroïdes est inférieure à un seuil
    #   (MIN_CENTROID_SEPARATION), cela signifie que K-Means a découpé un
    #   groupe homogène en sous-groupes artificiels → on bascule sur les
    #   règles métier aéronautiques.

    kmeans_model = model.stages[-1]
    centers = kmeans_model.clusterCenters()

    # Calculer la distance max entre paires de centroïdes
    import numpy as np
    max_centroid_dist = 0.0
    for i in range(len(centers)):
        for j in range(i + 1, len(centers)):
            d = float(np.linalg.norm(centers[i] - centers[j]))
            if d > max_centroid_dist:
                max_centroid_dist = d

    logger.info(
        "Distance max inter-centroïdes : %.4f  (seuil min = %.1f)",
        max_centroid_dist, MIN_CENTROID_SEPARATION,
    )

    clusters_are_meaningful = max_centroid_dist >= MIN_CENTROID_SEPARATION

    if clusters_are_meaningful:
        # ── Mode ML : les clusters sont bien séparés → labellisation auto ────
        logger.info("Clusters bien séparés → labellisation par K-Means.")

        cluster_stats = (
            df_clustered
            .groupBy("_ml_cluster_id")
            .agg(
                F.avg("velocity").alias("avg_velocity"),
                F.avg("baro_altitude").alias("avg_altitude"),
            )
            .collect()
        )

        cluster_stats_sorted = sorted(cluster_stats, key=lambda r: r["avg_altitude"])

        label_map = {}
        for i, row in enumerate(cluster_stats_sorted):
            cid = row["_ml_cluster_id"]
            avg_alt = row["avg_altitude"]
            avg_vel = row["avg_velocity"]

            if i == 0:
                label_map[cid] = "Takeoff / Landing"
            elif i == len(cluster_stats_sorted) - 1:
                label_map[cid] = "Cruise"
            else:
                label_map[cid] = "Climb / Descent"

            logger.info(
                "  Cluster %d → %s  (avg_alt=%.0f m, avg_vel=%.0f m/s)",
                cid, label_map[cid], avg_alt, avg_vel,
            )

        phase_expr = F.lit("Unknown")
        for cid, label in label_map.items():
            phase_expr = F.when(F.col("_ml_cluster_id") == cid, F.lit(label)).otherwise(phase_expr)

        df_clustered = (
            df_clustered
            .withColumn("flight_phase", phase_expr)
            .withColumn("flight_phase_id", F.col("_ml_cluster_id").cast(IntegerType()))
        )

    else:
        # ── Mode RÈGLES MÉTIER : clusters trop proches → fallback ────────────
        #   Seuils issus de standards aéronautiques (simplifiés) :
        #     - Au sol ou basse altitude (<300m) + vitesse faible (<60 m/s)
        #       → Takeoff / Landing
        #     - Altitude haute (>3000m) + vertical_rate quasi nul (|vr| < 2.5)
        #       → Cruise
        #     - Sinon → Climb / Descent
        logger.warning(
            "Clusters trop proches (dist=%.4f < %.1f) → "
            "fallback sur classification par règles métier aéronautiques.",
            max_centroid_dist, MIN_CENTROID_SEPARATION,
        )

        df_clustered = df_clustered.withColumn(
            "flight_phase",
            F.when(
                (F.col("baro_altitude") < 300) & (F.col("velocity") < 60),
                F.lit("Takeoff / Landing"),
            )
            .when(
                (F.col("baro_altitude") > 3000) & (F.abs(F.col("vertical_rate")) < 2.5),
                F.lit("Cruise"),
            )
            .otherwise(F.lit("Climb / Descent")),
        )
        df_clustered = df_clustered.withColumn(
            "flight_phase_id",
            F.when(F.col("flight_phase") == "Takeoff / Landing", F.lit(0))
             .when(F.col("flight_phase") == "Climb / Descent", F.lit(1))
             .otherwise(F.lit(2)),  # Cruise
        )

    logger.info(
        "Classification utilisée : %s",
        "K-Means" if clusters_are_meaningful else "Règles métier (fallback)",
    )

    # ── 7b. Détection d'anomalies (distance au centroïde) ────────────────────
    #   Fonctionne identiquement quel que soit le mode de classification.
    #   Pour chaque vol, on calcule la distance euclidienne entre son vecteur
    #   de features normalisé et le centroïde de son cluster K-Means.
    #   Les vols dont la distance dépasse (moyenne + ANOMALY_SIGMA × écart-type)
    #   sont marqués comme anomalies.

    # Construire des colonnes de centroïde par cluster_id
    center_velocity_expr = F.lit(0.0)
    center_altitude_expr = F.lit(0.0)
    center_vrate_expr = F.lit(0.0)

    scaler_model = model.stages[1]  # le StandardScalerModel
    means = scaler_model.mean.toArray()   # moyennes des features
    stds = scaler_model.std.toArray()     # écarts-types des features

    for cid, center in enumerate(centers):
        center_velocity_expr = F.when(
            F.col("_ml_cluster_id") == cid, F.lit(float(center[0]))
        ).otherwise(center_velocity_expr)
        center_altitude_expr = F.when(
            F.col("_ml_cluster_id") == cid, F.lit(float(center[1]))
        ).otherwise(center_altitude_expr)
        center_vrate_expr = F.when(
            F.col("_ml_cluster_id") == cid, F.lit(float(center[2]))
        ).otherwise(center_vrate_expr)

    # Normaliser les features du vol (même transformation que le scaler)
    norm_velocity = (F.col("velocity") - F.lit(float(means[0]))) / F.lit(float(stds[0]))
    norm_altitude = (F.col("baro_altitude") - F.lit(float(means[1]))) / F.lit(float(stds[1]))
    norm_vrate = (F.col("vertical_rate") - F.lit(float(means[2]))) / F.lit(float(stds[2]))

    # Distance euclidienne au centroïde (en espace normalisé)
    dist_to_centroid = F.sqrt(
        (norm_velocity - center_velocity_expr) ** 2
        + (norm_altitude - center_altitude_expr) ** 2
        + (norm_vrate - center_vrate_expr) ** 2
    )

    df_clustered = df_clustered.withColumn("_dist_to_centroid", dist_to_centroid)

    # Seuil d'anomalie : moyenne + ANOMALY_SIGMA * écart-type de la distribution des distances
    dist_stats = df_clustered.select(
        F.avg("_dist_to_centroid").alias("mean_dist"),
        F.stddev("_dist_to_centroid").alias("std_dist"),
    ).first()

    anomaly_threshold = dist_stats["mean_dist"] + ANOMALY_SIGMA * dist_stats["std_dist"]
    logger.info(
        "Seuil d'anomalie : %.4f  (mean=%.4f, std=%.4f, sigma=%.1f)",
        anomaly_threshold, dist_stats["mean_dist"], dist_stats["std_dist"], ANOMALY_SIGMA,
    )

    df_final = (
        df_clustered
        .withColumn(
            "is_anomaly",
            F.when(F.col("_dist_to_centroid") > F.lit(anomaly_threshold), F.lit(True))
             .otherwise(F.lit(False)),
        )
        .withColumn("anomaly_score", F.round(F.col("_dist_to_centroid"), 4))
        # Nettoyage : supprimer les colonnes internes ML
        .drop("_ml_features_raw", "_ml_features", "_ml_cluster_id", "_dist_to_centroid")
    )

    nb_anomalies = df_final.filter(F.col("is_anomaly") == True).count()  # noqa: E712
    logger.info("Anomalies détectées : %d / %d vols", nb_anomalies, df_final.count())

    # ── 8. Écriture Parquet (enriched) ───────────────────────────────────────
    out_dir = output_path("enriched", "sky_safe", "flights_weather")
    logger.info("Écriture Parquet enrichi vers : %s", out_dir)

    df_final.write.mode("overwrite").parquet(out_dir)

    logger.info("=== combine_data terminé (%d lignes) ===", df_final.count())
    return out_dir
