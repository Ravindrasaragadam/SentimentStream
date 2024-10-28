from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
import json
import configparser
import os

# Load configurations
config = configparser.ConfigParser()
storage_config_location = os.path.abspath('/opt/airflow/configs/storage_config.ini')
paths_config_location = os.path.abspath('/opt/airflow/configs/paths_config.ini')
config.read(storage_config_location)
config.read(paths_config_location)

STORAGE_TYPE = config['General']['storage_type']
OUTPUT_FILE_PATH = config['Paths']['output_file_path'] 

# PostgreSQL connection details
POSTGRES_HOST = config['Postgres']['postgres_host']
POSTGRES_USER = config['Postgres']['postgres_user']
POSTGRES_PASSWORD = config['Postgres']['postgres_password']
POSTGRES_DATABASE = config['Postgres']['postgres_database']

def initialize_postgres_database():
    if STORAGE_TYPE == 'postgres':
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DATABASE
            )
            cursor = connection.cursor()
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                link TEXT,
                description TEXT,
                date DATE,
                keyword VARCHAR(255),
                sentiment_score FLOAT
            );
            '''
            cursor.execute(create_table_query)
            connection.commit()
            cursor.close()
            connection.close()
            print("Table 'articles' created successfully in PostgreSQL.")
        except Exception as e:
            print(f"Error creating table: {e}")

class StorageHandler:
    def __init__(self):
        self.storage_type = STORAGE_TYPE
        self.connection = None

        # Configure database connections based on the storage type
        if self.storage_type == 'postgres':
            try:
                self.connection = psycopg2.connect(
                    host=POSTGRES_HOST,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    database=POSTGRES_DATABASE
                )
                self.cursor = self.connection.cursor()
            except Exception as e:
                raise Exception(f"Failed to connect to PostgreSQL: {e}")
        elif self.storage_type == 'local':
            # Local storage doesn't require a connection
            pass

    def save(self, data):
        """ Save the data to the configured storage. """
        print(OUTPUT_FILE_PATH)
        if self.storage_type == 'postgres':
            initialize_postgres_database()
            try:
                for article in data:
                    self.cursor.execute(
                        "INSERT INTO articles (title, link, description, date, keyword, sentiment_score) VALUES (%s, %s, %s, %s, %s, %s)",
                        (article['title'], article['link'], article['description'], article['date'], article['keyword'], article['sentiment_score'])
                    )
                self.connection.commit()
            except Exception as e:
                raise Exception(f"Error saving articles to PostgreSQL: {e}")
            finally:
                self.cursor.close()
        elif self.storage_type == 'local':
            with open(OUTPUT_FILE_PATH, 'w') as f:
                json.dump(data, f)
        else:
            print(self.storage_type)

    def close(self):
        """ Close the database connection if needed. """
        if self.connection:
            self.connection.close()

class SaveToStorageOperator(BaseOperator):
    @apply_defaults
    def __init__(self, parent_id=None, *args, **kwargs):
        super(SaveToStorageOperator, self).__init__(*args, **kwargs)
        self.parent_id = parent_id

    def execute(self, context):
        articles = context['task_instance'].xcom_pull(task_ids=self.parent_id)

        storage_handler = StorageHandler()  # Create an instance of the StorageHandler
        # Save articles to the configured storage
        storage_handler.save(articles)
        storage_handler.close()  # Close the connection
        return 'success'
