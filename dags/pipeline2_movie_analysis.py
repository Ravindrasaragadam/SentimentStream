from airflow import DAG
from datetime import timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from operators.movie_analysis_operator import MovieAnalysisOperator
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_pipeline1_success(**kwargs):
    # Pull the status from the XCom of the external task
    ti = kwargs['ti']
    # Use the DAG ID of the external pipeline
    status = ti.xcom_pull(dag_id='pipeline1_fetch_articles', task_ids='save_to_storage_task')
    if status == 'success':
        print("Pipeline 1 succeeded!")
        return True
    else:
        print("Pipeline 1 did not succeed.")
        return False

with DAG(
    'pipeline2_movie_analysis',
    default_args=default_args,
    schedule_interval='0 20 * * 1-5',
    catchup=False,
) as dag:

    check_pipeline1 = PythonOperator(
        task_id='check_pipeline1_success',
        python_callable=check_pipeline1_success,
        provide_context=True,
    )

    find_mean_age_task = MovieAnalysisOperator(
        movie_task_type='mean_age',
        task_id='find_mean_age',
        execution_timeout=timedelta(minutes=10)  # Set a timeout for the task
    )

    find_top_rated_movies_task = MovieAnalysisOperator(
        movie_task_type='top_rated_movies',
        task_id='find_top_rated_movies',
        execution_timeout=timedelta(minutes=10)
    )

    find_top_genres_task = MovieAnalysisOperator(
        movie_task_type='top_genres',
        task_id='find_top_genres',
        execution_timeout=timedelta(minutes=10)
    )

    find_similar_movies_task = MovieAnalysisOperator(
        movie_task_type='similar_movies',
        task_id='find_similar_movies',
        execution_timeout=timedelta(minutes=10)
    )

    end = DummyOperator(task_id='end')

    # Set the task dependencies
    check_pipeline1 >> [find_mean_age_task, find_top_rated_movies_task, find_top_genres_task, find_similar_movies_task] >> end
