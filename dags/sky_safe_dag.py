import sys

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(1, ".")
sys.path.insert(2, "./src")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract_flights import extract_flights_main
from extract_weather import extract_weather_main
from format_flights import format_flights_main
from format_weather import format_weather_main
from combine_spark import combine_data_main
from index_elastic import index_to_elastic_main


default_args = {
    'owner': 'sky_safe_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='sky_safe_pipeline',
    default_args=default_args,
    description='Pipeline Big Data : Vols (OpenSky) x Météo (Open-Meteo) -> Elasticsearch',
    schedule='*/2 * * * *',
    catchup=False,
    tags=['sky_safe', 'production'],
) as dag:

    extract_weather = PythonOperator(
        task_id='extract_weather_api',
        python_callable=extract_weather_main,
    )

    extract_flights = PythonOperator(
        task_id='extract_flights_api',
        python_callable=extract_flights_main,
    )

    format_flights_spark = PythonOperator(
        task_id='format_flights_spark',
        python_callable=format_flights_main,
    )

    format_weather_spark = PythonOperator(
        task_id='format_weather_spark',
        python_callable=format_weather_main,
    )

    combine_data_spark = PythonOperator(
        task_id='combine_data_spark',
        python_callable=combine_data_main,
        trigger_rule='all_success',  # Exécute si les deux tâches précédentes réussissent
    )

    index_to_elastic = PythonOperator(
        task_id='index_to_elastic',
        python_callable=index_to_elastic_main,
    )

    #  extract_flights ─► format_flights_spark ─┐
    #                                             ├─► combine_data_spark ─► index_to_elastic
    #  extract_weather ─► format_weather_spark ─┘

    extract_flights >> format_flights_spark
    extract_weather >> format_weather_spark
    [format_flights_spark, format_weather_spark] >> combine_data_spark >> index_to_elastic