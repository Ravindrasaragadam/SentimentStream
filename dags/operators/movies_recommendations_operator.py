import pandas as pd
import psycopg2
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.extras import Json
import configparser
import os
from scipy.spatial.distance import cosine

class MovieSimilarityOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(MovieSimilarityOperator, self).__init__(*args, **kwargs)
        self.config = configparser.ConfigParser()
        CONFIG_PATH = os.getenv("CONFIG_PATH", "configs/")
        PATHS_CONFIG_PATH = os.path.join(CONFIG_PATH, 'paths_config.ini')
        self.config.read(PATHS_CONFIG_PATH)

    def load_data(self, file_path, separator='\t'):
        """Loads movie ratings data from a CSV file."""
        return pd.read_csv(file_path, sep=separator, header=None, encoding='ISO-8859-1')

    def create_user_item_matrix(self, ratings):
        """Creates a user-item rating matrix."""
        user_item_matrix = ratings.pivot_table(index='user_id', columns='movie_id', values='rating')
        return user_item_matrix

    def calculate_item_similarity(self, user_item_matrix):
        """Calculates similarity between items (movies) using Pearson correlation."""
        item_similarity = user_item_matrix.corr(method='pearson')
        return item_similarity

    def calculate_genre_similarity(self, movie1_genres, movie2_genres):
        """Calculates genre-based similarity between two movies."""
        return 1 - cosine(movie1_genres, movie2_genres)
    
    def calculate_cooccurrence_strength(self, movie1_id, movie2_id, ratings_matrix):
        """Calculates the co-occurrence strength between two movies."""
        users_rated_both = ratings_matrix.index[(ratings_matrix[movie1_id] > 0) & (ratings_matrix[movie2_id] > 0)]
        return len(users_rated_both)

    def recommend_similar_movies(self, movie_title, similarity_matrix, user_item_matrix, movie_metadata, top_n=10, similarity_threshold=0.95):
        """Recommends top N similar movies with scores and strengths, including movie ID and title.

        Args:
            movie_title: The title of the target movie.
            similarity_matrix: The similarity matrix.
            user_item_matrix: The user-item rating matrix.
            movie_metadata: The movie metadata DataFrame.
            top_n: The number of top similar movies to return.
            similarity_threshold: The minimum similarity threshold.

        Returns:
            A list of tuples, where each tuple contains the movie_id, movie_title, similar_movie_id, similar_movie_title, combined similarity score, and co-occurrence strength.
        """

        # Find the movie ID based on the title
        movie_id = movie_metadata[movie_metadata['movie_title'] == movie_title]['movie_id'].values[0]

        similar_movies = similarity_matrix.loc[movie_id].sort_values(ascending=False)
        similar_movies = similar_movies[similar_movies > similarity_threshold]  # Filter by similarity threshold

        similar_movies = similar_movies.reset_index()
        similar_movies.columns = ['similar_movie_id', 'content_based_similarity']

        # Extract genre data for the selected movie
        movie1_genres = movie_metadata[movie_metadata['movie_id'] == movie_id].iloc[0, 5:].values

        # Calculate genre-based similarity
        similar_movies['genre_similarity'] = similar_movies['similar_movie_id'].apply(
            lambda x: self.calculate_genre_similarity(
                movie1_genres,
                movie_metadata[movie_metadata['movie_id'] == x].iloc[0, 5:].values
            )
        )

        # Combine content-based and genre-based similarities
        similar_movies['combined_similarity'] = 0.7 * similar_movies['content_based_similarity'] + 0.3 * similar_movies['genre_similarity']

        # Calculate co-occurrence strength
        similar_movies['cooccurrence_strength'] = similar_movies['similar_movie_id'].apply(
            lambda x: self.calculate_cooccurrence_strength(movie_id, x, user_item_matrix)
        )

        similar_movies = similar_movies.sort_values(by=['combined_similarity', 'cooccurrence_strength'], ascending=False)
        similar_movies = similar_movies.head(top_n)

        # Map movie IDs to titles
        similar_movies['similar_movie_title'] = similar_movies['similar_movie_id'].map(movie_metadata.set_index('movie_id')['movie_title'])

        # Include original movie ID and title in the recommendations list
        return [(movie_id, movie_title, row['similar_movie_id'], row['similar_movie_title'], row['combined_similarity'], row['cooccurrence_strength'])
                for _, row in similar_movies.iterrows() if row['similar_movie_id']!=movie_id]

    def save_to_postgres(self, table_name, recommendations):
        """Saves recommendations to PostgreSQL with defined schema."""

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost:5347"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            database=os.getenv("POSTGRES_DATABASE", "airflow")
        )
        cursor = conn.cursor()

        # Define schema for similar_movies table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS similar_movies (
            id SERIAL PRIMARY KEY,
            movie_id INTEGER,
            movie_title VARCHAR(255),
            similar_movie_id INTEGER,
            similar_movie_title VARCHAR(255),
            similarity_score FLOAT,
            cooccurrence_strength INTEGER
        )
        """
        cursor.execute(create_table_query)

        # Insert data into the similar_movies table
        for movie_id, movie_title, similar_movie_id, similar_movie_title, similarity_score, cooccurrence_strength in recommendations:
            insert_query = f"INSERT INTO {table_name} (movie_id, movie_title, similar_movie_id, similar_movie_title, similarity_score, cooccurrence_strength) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (int(movie_id), movie_title, int(similar_movie_id), similar_movie_title, float(similarity_score), int(cooccurrence_strength)))

        conn.commit()
        cursor.close()
        conn.close()

    def execute(self, context):
        # Load the data
        ratings_data = self.load_data(self.config['Paths']['ratings_data_path'], separator='\t')
        ratings_data.columns = ['user_id', 'movie_id', 'rating', 'timestamp']

        # Load movie metadata including genres
        movie_metadata = pd.read_csv(self.config['Paths']['movies_data_path'], sep='|', header=None, encoding='ISO-8859-1')
        movie_metadata.columns = ['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 
                                    'unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy', 
                                    'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 
                                    'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']

        # Create user-item matrix and calculate item similarity
        user_item_matrix = self.create_user_item_matrix(ratings_data)
        item_similarity = self.calculate_item_similarity(user_item_matrix)

        # Recommend similar movies and save the results in PostgreSQL
        for movie_title in movie_metadata['movie_title'].unique():
            recommendations = self.recommend_similar_movies(movie_title, item_similarity, user_item_matrix, movie_metadata)
            self.save_to_postgres("similar_movies", recommendations)

        self.log.info("Successfully stored similar movies in the PostgreSQL 'similar_movies' table.")
