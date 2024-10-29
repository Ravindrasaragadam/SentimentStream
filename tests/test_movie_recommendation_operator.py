import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from dags.operators.movies_recommendations_operator import MovieSimilarityOperator  # Adjust the import based on your module structure

@pytest.fixture
def movie_similarity_operator():
    """Fixture to create an instance of MovieSimilarityOperator."""
    return MovieSimilarityOperator(task_id='test_operator')


@pytest.fixture
def mock_ratings_data():
    """Fixture to provide mock ratings data for testing."""
    return pd.DataFrame({
        'user_id': [1, 2, 1, 2],
        'movie_id': [101, 101, 102, 103],
        'rating': [4.0, 5.0, 3.0, 4.0]
    })


@pytest.fixture
def mock_movie_metadata():
    """Fixture to provide mock movie metadata for testing."""
    return pd.DataFrame({
        'movie_id': [101, 102, 103],
        'movie_title': ['Movie A', 'Movie B', 'Movie C'],
        'release_date': ['2000-01-01', '2001-01-01', '2002-01-01'],
        'Action': [1, 0, 1],
        'Adventure': [0, 1, 0],
        'Comedy': [0, 0, 1]
    })


@patch('pandas.read_csv')
def test_load_data(mock_read_csv, movie_similarity_operator, mock_ratings_data):
    """Test the load_data method."""
    mock_read_csv.return_value = mock_ratings_data
    data = movie_similarity_operator.load_data('mock_path')
    pd.testing.assert_frame_equal(data, mock_ratings_data)


def test_calculate_item_similarity(movie_similarity_operator, mock_ratings_data):
    """Test the calculate_item_similarity method."""
    user_item_matrix = movie_similarity_operator.create_user_item_matrix(mock_ratings_data)
    item_similarity = movie_similarity_operator.calculate_item_similarity(user_item_matrix)

    assert item_similarity.shape[0] == user_item_matrix.shape[1]  # Number of items should match
    assert item_similarity.shape[1] == user_item_matrix.shape[1]


def test_calculate_genre_similarity(movie_similarity_operator):
    """Test the calculate_genre_similarity method."""
    movie1_genres = [1, 0, 0]  # Movie A genres
    movie2_genres = [1, 0, 1]  # Movie C genres
    similarity = movie_similarity_operator.calculate_genre_similarity(movie1_genres, movie2_genres)
    assert similarity >= 0  # Similarity should be a non-negative value


if __name__ == "__main__":
    pytest.main()