import pytest
from dags.operators.fetch_articles_operator import FetchArticlesOperator

@pytest.fixture
def fetch_operator():
    return FetchArticlesOperator(sources=["dummy"],
        keywords=["HDFC"],
        max_results=1,
        task_id='fetch_articles_task')

def test_fetch_articles(fetch_operator):
    articles = fetch_operator.execute({})
    assert isinstance(articles, list), "Fetched articles should be a list"
