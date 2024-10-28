import pytest
from dags.operators.generate_sentiment_score_operator import GenerateSentimentScoreOperator

@pytest.fixture
def sentiment_operator():
    return GenerateSentimentScoreOperator(parent_id='clean_articles_task',
        task_id='generate_sentiment_score_task')

def test_generate_sentiment_score(sentiment_operator):
    sample_article = "The company has shown significant growth in recent quarters."
    score = sentiment_operator.generate_sentiment_score(sample_article)
    assert isinstance(score, float), "Sentiment score should be a float"
    assert 0 <= score <= 1.0, "Sentiment score should be within 0 to 1"
