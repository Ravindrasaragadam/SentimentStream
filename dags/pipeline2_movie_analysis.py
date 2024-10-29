from airflow import DAG
from datetime import timedelta, datetime
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from operators.movie_analysis_operator import MovieAnalysisOperator
from operators.movies_recommendations_operator import MovieSimilarityOperator
from operators.genere_operator import UserOccupationGenreAnalysisOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Start date for the DAG
    'email_on_failure': False,    # Disable email alerts on failure
    'retries': 1,                 # Number of retries before failing
    'retry_delay': timedelta(minutes=1),  # Delay between retries
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

# Define the DAG
with DAG(
    'pipeline2_movie_analysis',
    default_args=default_args,
    schedule_interval='0 20 * * 1-5',  # Schedule to run at 20:00 from Monday to Friday
    catchup=False,  # Do not perform backfill
) as dag:

    check_pipeline1 = PythonOperator(
        task_id='check_pipeline1_success',
        python_callable=check_pipeline1_success,
        provide_context=True,
    )

    # Task to find the mean age of movie watchers
    find_mean_age_task = MovieAnalysisOperator(
        movie_task_type='mean_age',
        task_id='find_mean_age',
        execution_timeout=timedelta(minutes=10)  
    )

    # Task to find the top-rated movies
    find_top_rated_movies_task = MovieAnalysisOperator(
        movie_task_type='top_rated_movies',
        task_id='find_top_rated_movies',
        execution_timeout=timedelta(minutes=10)
    )

    # Task to find the top genres based on user occupation
    find_top_genres_task = UserOccupationGenreAnalysisOperator(
        task_id='find_top_genres',
        execution_timeout=timedelta(minutes=10)
    )

    # Task to find similar movies
    find_similar_movies_task = MovieSimilarityOperator(
        task_id='find_similar_movies',
        execution_timeout=timedelta(minutes=10)
    )

    # Dummy task to signify the end of the pipeline
    end = DummyOperator(task_id='end')

    # Set the task dependencies
    check_pipeline1 >> [find_mean_age_task, find_top_rated_movies_task, find_top_genres_task, find_similar_movies_task] >> end
