from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CleanArticlesOperator(BaseOperator):
    """
    This operator cleans articles retrieved from a previous task.

    It fetches articles from the XCom of the provided parent task ID, removes
    duplicates based on title, and returns a list of cleaned articles.
    """

    @apply_defaults
    def __init__(self, parent_id=None, *args, **kwargs):
        """
        Initializes the CleanArticlesOperator.

        Args:
            parent_id (str): ID of the parent task that generated the articles.
            *args: Arguments passed to the BaseOperator constructor.
            **kwargs: Keyword arguments passed to the BaseOperator constructor.
        """
        super(CleanArticlesOperator, self).__init__(*args, **kwargs)
        self.parent_id = parent_id

    def execute(self, context):
        """
        Executes the cleaning logic.

        Fetches articles from the XCom of the parent task, identifies and removes
        duplicates based on title, and returns a list of cleaned articles.

        Args:
            context (dict): Airflow context dictionary.

        Returns:
            list: A list of cleaned articles (dictionaries).
        """

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