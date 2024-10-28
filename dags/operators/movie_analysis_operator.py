from airflow.models import BaseOperator
import pandas as pd
import configparser
import os
import psycopg2
import json

# Load configurations
CONFIG_PATH = "CONFIG_PATH"
config = configparser.ConfigParser()
storage_config_location = os.path.abspath(os.environ.get(CONFIG_PATH, 'configs/')+'storage_config.ini')
paths_config_location = os.path.abspath(os.environ.get(CONFIG_PATH, 'configs/')+'paths_config.ini')
config.read(storage_config_location)
config.read(paths_config_location)
print(storage_config_location)
STORAGE_TYPE = config['General']['storage_type']
OUTPUT_FILE_PATH = config['Paths']['output_file_path'] 

# PostgreSQL connection details
POSTGRES_HOST = config['Postgres']['postgres_host']
POSTGRES_USER = config['Postgres']['postgres_user']
POSTGRES_PASSWORD = config['Postgres']['postgres_password']
POSTGRES_DATABASE = config['Postgres']['postgres_database']

class MovieAnalysisOperator(BaseOperator):
    def __init__(self, movie_task_type=None, postgres_conn_id='postgres_default', *args, **kwargs):
        super(MovieAnalysisOperator, self).__init__(*args, **kwargs)
        self.movie_task_type = movie_task_type
        self.postgres_conn_id = postgres_conn_id
        
        # Load configuration
        config = configparser.ConfigParser()
        paths_config_location = os.path.abspath(os.environ.get(CONFIG_PATH, 'configs/')+'paths_config.ini')
        config.read(paths_config_location)
        print(paths_config_location)
        
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
        elif self.movie_task_type == "top_genres":
            self.find_top_genres()
        elif self.movie_task_type == "similar_movies":
            self.find_similar_movies()

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
        elif table_name == 'top_genres':
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS top_genres (
                id SERIAL PRIMARY KEY,
                genre VARCHAR(255),
                count INTEGER
            )
            """
        elif table_name == 'similar_movies':
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS similar_movies (
                id SERIAL PRIMARY KEY,
                movie_title VARCHAR(255),
                similar_movies JSONB
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
            elif table_name == 'top_genres':
                insert_query = f"INSERT INTO {table_name} (genre, count) VALUES (%s, %s)"
                cursor.execute(insert_query, (row['genre'], row['count']))
            elif table_name == 'similar_movies':
                insert_query = f"INSERT INTO {table_name} (movie_title, similar_movies) VALUES (%s, %s)"
                cursor.execute(insert_query, (row['movie_title'], row['similar_movies']))

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

    def find_top_genres(self):
        try:
            ratings_df = pd.read_csv(self.ratings_data_path, sep='\t', header=None, encoding='ISO-8859-1')
            movies_df = pd.read_csv(self.movies_data_path, sep='|', header=None, encoding='ISO-8859-1')

            ratings_df.columns = ['user_id', 'movie_id', 'rating', 'timestamp']
            movies_df.columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + self.genre_names
            
            # Prepare data for genre analysis
            genres = movies_df.columns[5:]  # Adjust this index based on your actual data
            genres_df = movies_df.melt(id_vars=['movie_id', 'title'], value_vars=genres,
                                        var_name='genre', value_name='is_genre')
            top_genres_df = genres_df[genres_df['is_genre'] == 1].groupby('genre').size().reset_index(name='count')
            top_genres_df = top_genres_df.nlargest(10, 'count')

            self.log.info(top_genres_df)  # Log the result
            
            # Save to PostgreSQL
            self.save_to_postgres('top_genres', top_genres_df)
        except Exception as e:
            self.log.error(f"Error in find_top_genres: {e}")

    def find_similar_movies(self):
        try:
            # Load movies data
            movies_df = pd.read_csv(self.movies_data_path, sep='|', header=None, encoding='ISO-8859-1')
            movies_df.columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + self.genre_names
            
            # Find similar movies based on genre
            genre_columns = movies_df.columns[5:]  # Adjust this index based on your actual data
            similar_movies_list = []
            for _, row in movies_df.iterrows():
                similar_movies = movies_df[movies_df[genre_columns].eq(row[genre_columns]).any(axis=1)]
                similar_movies_list.append((row['title'], similar_movies[['title']].to_dict(orient='records')))
            
            # Convert list to DataFrame
            similar_movies_df = pd.DataFrame(similar_movies_list, columns=['movie_title', 'similar_movies'])
            
            # Convert 'similar_movies' column to JSON strings
            similar_movies_df['similar_movies'] = similar_movies_df['similar_movies'].apply(json.dumps)
            
            # Save to PostgreSQL
            self.save_to_postgres('similar_movies', similar_movies_df)
        except Exception as e:
            self.log.error(f"Error in find_similar_movies: {e}")
