import configparser
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from operators.fetch_articles_operator import FetchArticlesOperator
from operators.clean_articles_operator import CleanArticlesOperator
from operators.generate_sentiment_score_operator import GenerateSentimentScoreOperator
from operators.save_to_storage_operator import SaveToStorageOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Load configuration
source_config_location = os.path.abspath('/opt/airflow/configs/source_config.ini')
config = configparser.ConfigParser()

config.read(source_config_location)

sources = config['Sources']['sources'].split(', ')
keywords = config['Keywords']['keywords'].split(', ')
max_results = int(config.get('Settings', 'max_results', fallback=5))  # Example for adding max_results

with DAG(
    'pipeline1_fetch_articles',
    default_args=default_args,
    description='Fetch and analyze articles about HDFC and Tata Motors',
    schedule_interval='0 19 * * 1-5',  # Every working day at 7 PM
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    fetch_articles_task = FetchArticlesOperator(
        sources=sources,
        keywords=keywords,
        max_results=max_results,
        task_id='fetch_articles_task'
    )

    clean_articles_task = CleanArticlesOperator(
        parent_id='fetch_articles_task',
        task_id='clean_articles_task'
    )

    generate_sentiment_score_task = GenerateSentimentScoreOperator(
        parent_id='clean_articles_task',
        task_id='generate_sentiment_score_task'
    )

    save_to_storage_task = SaveToStorageOperator(
        parent_id='generate_sentiment_score_task',
        task_id='save_to_storage_task'
    )

    end = DummyOperator(task_id='end')

    start >> fetch_articles_task >> clean_articles_task >> generate_sentiment_score_task >> save_to_storage_task >> end
