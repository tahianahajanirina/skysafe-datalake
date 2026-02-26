# 1. On part de l'image officielle Airflow
FROM apache/airflow:2.7.1

# 2. Installation de Java + curl (requis par PySpark et le téléchargement des JARs)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 3. Télécharger les JARs Hadoop AWS pour le support S3A dans Spark
RUN mkdir -p /opt/spark-jars \
    && curl -sSL -o /opt/spark-jars/hadoop-aws-3.3.4.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -sSL -o /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 4. Pré-créer le dossier data monté en volume (permissions ouvertes)
RUN mkdir -p /opt/airflow/data && chmod 777 /opt/airflow/data

# 5. On revient à l'utilisateur Airflow (sécurité)
USER airflow

# 6. On copie et installe les dépendances Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt