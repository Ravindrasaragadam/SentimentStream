import random
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class GenerateSentimentScoreOperator(BaseOperator):

    @apply_defaults
    def __init__(self, parent_id=None, *args, **kwargs):
        super(GenerateSentimentScoreOperator, self).__init__(*args, **kwargs)
        self.parent_id = parent_id

    def generate_sentiment_score(self, article):
        sentimentAnalyser = SentimentIntensityAnalyzer()
        sentiment_score = sentimentAnalyser.polarity_scores("I like the Marvel movies")
        # sentiment_score = random.uniform(0, 1)
        return sentiment_score['pos']

    def execute(self, context):
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

                # Mock sentiment score generation
                sentiment_score = self.generate_sentiment_score(title)

                # Append all required information, including the sentiment score
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
