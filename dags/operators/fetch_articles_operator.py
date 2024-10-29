import requests
from bs4 import BeautifulSoup
from airflow.models import BaseOperator
from datetime import datetime
import logging
import time

class FetchArticlesOperator(BaseOperator):
    """
    This operator fetches articles from specified sources based on keywords.

    It takes a list of sources (e.g., Finshots, YourStory) and keywords as input.
    For each keyword, it iterates through the sources and fetches articles
    containing the keyword. Finally, it returns a list of dictionaries containing
    article details like title, link, source, etc.
    """

    def __init__(self, sources=None, keywords=None, max_results=5, *args, **kwargs):
        """
        Initializes the FetchArticlesOperator.

        Args:
            sources (list, optional): List of source names (e.g., Finshots, YourStory). Defaults to None.
            keywords (list, optional): List of keywords to search for in articles. Defaults to None.
            max_results (int, optional): Maximum number of articles to fetch per source/keyword combination. Defaults to 5.
            *args: Arguments passed to the BaseOperator constructor.
            **kwargs: Keyword arguments passed to the BaseOperator constructor.
        """
        super(FetchArticlesOperator, self).__init__(*args, **kwargs)
        self.sources = sources
        self.keywords = keywords
        self.max_results = max_results

    def fetch_finshots_articles(self, keyword):
        """
        Fetches articles from Finshots based on the given keyword.

        This method iterates through pages of Finshots search results
        until the maximum number of articles is reached or no more articles
        are found on subsequent pages.

        Args:
            keyword (str): Keyword to search for in Finshots articles.

        Returns:
            list: A list of dictionaries containing article details (title, link, etc.).
        """
        articles = []
        page = 1
        while len(articles) < self.max_results:
            url = f"https://finshots.in/archive/page/{page}/"
            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise an error for bad responses
            except requests.RequestException as e:
                self.log.error(f"Error fetching Finshots articles: {e}")
                break

            soup = BeautifulSoup(response.text, "html.parser")

            for article in soup.find_all("a", href=True):
                title = article.get_text().strip()
                link = article["href"]

                if keyword.lower() in title.lower():
                    articles.append({
                        'title': title,
                        'link': f"https://finshots.in{link}",
                        'keyword': keyword,
                        'source': 'Finshots',
                        'description': "No description available",
                        'date': datetime.now().isoformat(),
                        'run_date': datetime.now().isoformat()
                    })

                    if len(articles) >= self.max_results:
                        break

            page += 1
            if not soup.find_all("a", href=True):
                break
            time.sleep(1)  # Sleep for a second to avoid rate limiting
        return articles

    def fetch_yourstory_articles(self, keyword):
        """
        Fetches articles from YourStory based on the given keyword.

        This method uses the YourStory API to retrieve a list of stories
        matching the keyword. It then extracts relevant details from each story
        and returns them as a list of dictionaries.

        Args:
            keyword (str): Keyword to search for in YourStory articles.

        Returns:
            list: A list of dictionaries containing article details (title, link, etc.).
        """
        articles = []
        url = f"https://yourstory.com/api/v2/tag/stories?slug={keyword.lower().replace(' ','-')}"

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            stories = data.get('stories', [])

            for story in stories[:self.max_results]:
                title = story.get('title', 'No title')
                link = f"https://yourstory.com/{story.get('slug', '')}"
                date = story.get('publishedAt', datetime.now().isoformat())
                description = story.get('metadata', {}).get('excerpt', 'No description available')

                articles.append({
                    'title': title,
                    'link': link,
                    'keyword': keyword,
                    'date': date,
                    'description': description,
                    'source': 'YourStory',
                    'run_date': datetime.now().isoformat()
                })
        except requests.RequestException as e:
            self.log.error(f"Error fetching YourStory articles: {e}")
        
        return articles

    def execute(self, context):
        """
        Executes the operator to fetch articles based on the keywords and sources.

        This method iterates over the keywords and sources, fetching articles
        from each source for each keyword. The fetched articles are appended
        to a list and returned.

        Args:
            context (dict): Airflow context dictionary.

        Returns:
            list: A list of dictionaries containing fetched articles.
        """
        articles = []
        for keyword in self.keywords:
            for source in self.sources:
                self.log.info(f"Fetching articles for keyword '{keyword}' from source: {source}")
                if source.lower() == 'finshots':
                    articles.extend(self.fetch_finshots_articles(keyword))
                elif source.lower() == 'yourstory':
                    articles.extend(self.fetch_yourstory_articles(keyword))

        self.log.info(f"Total articles fetched: {len(articles)}")
        return articles