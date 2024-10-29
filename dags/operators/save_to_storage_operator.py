from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
import json
import os
import configparser

# Load configuration
config = configparser.ConfigParser()
CONFIG_PATH = os.getenv("CONFIG_PATH", "configs/")
PATHS_CONFIG_PATH = os.path.join(CONFIG_PATH, 'paths_config.ini')
config.read(PATHS_CONFIG_PATH)
output_file_path = config['Paths']['output_file_path']

# PostgreSQL connection details
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost:5347"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "database": os.getenv("POSTGRES_DATABASE", "airflow")
}

def initialize_postgres_database():
    try:
        with psycopg2.connect(**POSTGRES_CONFIG) as connection:
            with connection.cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS articles (
                        id SERIAL PRIMARY KEY,
                        title VARCHAR(255),
                        link TEXT,
                        description TEXT,
                        date DATE,
                        keyword VARCHAR(255),
                        sentiment_score FLOAT
                    );
                ''')
            print("Table 'articles' created successfully in PostgreSQL.")
    except Exception as e:
        print(f"Error creating table: {e}")

class StorageHandler:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**POSTGRES_CONFIG)
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise Exception(f"Failed to connect to PostgreSQL: {e}")

    def save(self, data):
        """ Save the data to the configured storage. """
        initialize_postgres_database()
        try:
            insert_query = "INSERT INTO articles (title, link, description, date, keyword, sentiment_score) VALUES (%s, %s, %s, %s, %s, %s)"
            for article in data:
                self.cursor.execute(insert_query, (
                    article['title'], 
                    article['link'], 
                    article['description'], 
                    article['date'], 
                    article['keyword'], 
                    article['sentiment_score']
                ))
            self.connection.commit()
        except Exception as e:
            self.save_to_file(data)
            raise Exception(f"Error saving articles to PostgreSQL: {e}")

    def save_to_file(self, data):
        """ Save the data to a local file. """
        print("Saving to local!")
        with open(output_file_path, 'w') as f:
            json.dump(data, f)

    def close(self):
        """ Close the database connection if needed. """
        if self.connection:
            self.connection.close()

class SaveToStorageOperator(BaseOperator):
    @apply_defaults
    def __init__(self, parent_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parent_id = parent_id

    def execute(self, context):
        articles = context['task_instance'].xcom_pull(task_ids=self.parent_id)
        storage_handler = StorageHandler()
        storage_handler.save(articles)
        return 'success'
