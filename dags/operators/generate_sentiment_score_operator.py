import random
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class GenerateSentimentScoreOperator(BaseOperator):
    """
    This operator generates sentiment scores for articles retrieved from a previous task.

    It fetches articles from the XCom of the provided parent task ID,
    analyzes the sentiment of the article title (or provided content),
    and returns a list of dictionaries containing the sentiment score along
    with other article details.
    """

    @apply_defaults
    def __init__(self, parent_id=None, *args, **kwargs):
        """
        Initializes the GenerateSentimentScoreOperator.

        Args:
            parent_id (str): ID of the parent task that generated the articles.
            *args: Arguments passed to the BaseOperator constructor.
            **kwargs: Keyword arguments passed to the BaseOperator constructor.
        """
        super(GenerateSentimentScoreOperator, self).__init__(*args, **kwargs)
        self.parent_id = parent_id

    def generate_sentiment_score(self, article):
        """
        Analyzes the sentiment of the provided text using VaderSentiment.

        This method creates a SentimentIntensityAnalyzer object
        and uses it to analyze the sentiment of the given text.
        Currently, it analyzes a placeholder string, but should be replaced
        with the actual article text (e.g., article.get('content', '')).

        Args:
            article (dict): A dictionary containing article details.

        Returns:
            float: The positive sentiment score (between 0 and 1) for the analyzed text.
        """
        sentimentAnalyser = SentimentIntensityAnalyzer()
        # Replace with actual article text retrieval (e.g., article.get('content', ''))
        sentiment_score = sentimentAnalyser.polarity_scores("I like the Marvel movies")
        return sentiment_score['pos']

    def execute(self, context):
        """
        Executes the sentiment score generation logic.

        Fetches articles from the XCom of the parent task, iterates over them,
        analyzes the sentiment of the title (or provided content), and returns
        a list of dictionaries with sentiment scores and other article details.

        Args:
            context (dict): Airflow context dictionary.

        Returns:
            list: A list of dictionaries containing article details and sentiment scores.
        """
        articles = context['task_instance'].xcom_pull(task_ids=self.parent_id)

        if not articles:
            self.log.warning("No articles found for sentiment score generation.")
            return []

        results = []
        for article in articles:
            title = article.get('title')
            if title:
                # Log the processing of the article
                self.log.info(f"Generating sentiment score for article: {title}")

                # Analyze sentiment of the title (or provided content)
                sentiment_score = self.generate_sentiment_score(title)

                # Append article details and sentiment score
                results.append({
                    'keyword': article.get('keyword', ''),
                    'title': title,
                    'link': article['link'],
                    'sentiment_score': sentiment_score,
                    'description': article.get('description', ''),
                    'date': article.get('date', ''),
                    'run_date': article.get('run_date', '')
                })
            else:
                self.log.warning("Article title is missing, skipping this article.")

        self.log.info(f"Generated sentiment scores for {len(results)} articles.")
        return results