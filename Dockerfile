# 1. On part de l'image officielle Airflow
FROM apache/airflow:2.7.1

# 2. Installation de Java (requis par PySpark) — nécessite les droits root
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 3. Pré-créer le dossier data monté en volume (permissions ouvertes)
RUN mkdir -p /opt/airflow/data && chmod 777 /opt/airflow/data

# 4. On revient à l'utilisateur Airflow (sécurité)
USER airflow

# 5. On copie et installe les dépendances Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt