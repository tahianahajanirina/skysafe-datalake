# 1. On part de l'image officielle Airflow (la voiture de location)
FROM apache/airflow:2.7.1

# 2. On copie notre liste de courses à l'intérieur de l'image
COPY requirements.txt /requirements.txt

# 3. On installe tous les outils (boto3, pyspark, etc.)
RUN pip install --no-cache-dir -r /requirements.txt