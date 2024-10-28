from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CleanArticlesOperator(BaseOperator):
    @apply_defaults
    def __init__(self, parent_id=None, *args, **kwargs):
        super(CleanArticlesOperator, self).__init__(*args, **kwargs)
        self.parent_id = parent_id

    def execute(self, context):
        # Fetch articles from the previous task's XCom
        articles = context['task_instance'].xcom_pull(task_ids=self.parent_id)
        
        if not articles:
            self.log.warning("No articles found for cleaning.")
            return []

        self.log.info(f"Found {len(articles)} articles for cleaning.")

        cleaned_articles = []
        unique_titles = set()  # To keep track of unique titles

        for article in articles:
            title = article.get('title', None)  # Safely get the title

            # Check for uniqueness and existence of title
            if title and title not in unique_titles:
                unique_titles.add(title)
                cleaned_articles.append(article)  # Append the full article dictionary
        
        self.log.info(f"Cleaned articles, unique count: {len(cleaned_articles)}")
        return cleaned_articles
