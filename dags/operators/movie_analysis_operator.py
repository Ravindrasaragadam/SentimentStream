from airflow.models import BaseOperator
import pandas as pd
import configparser
import os
import psycopg2

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost:5347"),
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow"),
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow"),
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "airflow")

class MovieAnalysisOperator(BaseOperator):
    def __init__(self, movie_task_type=None, *args, **kwargs):
        super(MovieAnalysisOperator, self).__init__(*args, **kwargs)
        self.movie_task_type = movie_task_type
        # Load configuration
        config = configparser.ConfigParser()
        CONFIG_PATH = os.getenv("CONFIG_PATH", "configs/")
        PATHS_CONFIG_PATH = os.path.join(CONFIG_PATH, 'paths_config.ini')
        config.read(PATHS_CONFIG_PATH)
        self.user_data_path = config['Paths']['user_data_path']  # Path to u.user file
        self.ratings_data_path = config['Paths']['ratings_data_path']  # Path to u.data file
        self.movies_data_path = config['Paths']['movies_data_path']  # Path to u.item file
        self.genre_names = [
                'unknown', 'Action', 'Adventure', 'Animation', 'Children\'s', 'Comedy', 'Crime',
                'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
                'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'
            ]

    def execute(self, context):
        if self.movie_task_type == "mean_age":
            self.find_mean_age()
        elif self.movie_task_type == "top_rated_movies":
            self.find_top_rated_movies()

    def save_to_postgres(self, table_name, df):
        """Save DataFrame to PostgreSQL with defined schema."""
        conn = psycopg2.connect(
                host=POSTGRES_HOST,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DATABASE
            )
        cursor = conn.cursor()
        print(POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST)
        # Define schemas for different tables
        if table_name == 'mean_age':
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS mean_age (
                id SERIAL PRIMARY KEY,
                occupation VARCHAR(255),
                mean_age FLOAT
            )
            """
        elif table_name == 'top_rated_movies':
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS top_rated_movies (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER,
                title VARCHAR(255),
                avg_rating FLOAT,
                rating_count INTEGER
            )
            """
        cursor.execute(create_table_query)
        
        # Insert data
        for index, row in df.iterrows():
            if table_name == 'mean_age':
                insert_query = f"INSERT INTO {table_name} (occupation, mean_age) VALUES (%s, %s)"
                cursor.execute(insert_query, (row['occupation'], row['mean_age']))
            elif table_name == 'top_rated_movies':
                insert_query = f"INSERT INTO {table_name} (movie_id, title, avg_rating, rating_count) VALUES (%s, %s, %s, %s)"
                cursor.execute(insert_query, (row['movie_id'], row['title'], row['avg_rating'], row['rating_count']))

        conn.commit()
        cursor.close()
        conn.close()

    def find_mean_age(self):
        try:
            users_df = pd.read_csv(self.user_data_path, sep='|', header=None, encoding='ISO-8859-1')
            users_df.columns = ['user_id', 'age', 'gender', 'occupation', 'zip_code']
            mean_age_df = users_df.groupby('occupation')['age'].mean().reset_index()
            mean_age_df.columns = ['occupation', 'mean_age']
            self.log.info(mean_age_df)  # Log the result
            
            # Save to PostgreSQL
            self.save_to_postgres('mean_age', mean_age_df)
        except Exception as e:
            self.log.error(f"Error in find_mean_age: {e}")

    def find_top_rated_movies(self):
        try:
            ratings_df = pd.read_csv(self.ratings_data_path, sep='\t', header=None, encoding='ISO-8859-1')
            movies_df = pd.read_csv(self.movies_data_path, sep='|', header=None, encoding='ISO-8859-1')

            ratings_df.columns = ['user_id', 'movie_id', 'rating', 'timestamp']
            top_movies_df = ratings_df.groupby('movie_id').agg(
                avg_rating=('rating', 'mean'),
                rating_count=('user_id', 'count')
            ).reset_index()

            # Filter top rated movies
            top_movies_df = top_movies_df[top_movies_df['rating_count'] >= 35].nlargest(20, 'avg_rating')
            movies_df.columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + self.genre_names
            
            # Merge with movie titles
            top_movies_with_titles = pd.merge(top_movies_df, movies_df[['movie_id', 'title']], on='movie_id')
            self.log.info(top_movies_with_titles[['title', 'avg_rating']])  # Log the result
            
            # Save to PostgreSQL
            self.save_to_postgres('top_rated_movies', top_movies_with_titles)
        except Exception as e:
            self.log.error(f"Error in find_top_rated_movies: {e}")
