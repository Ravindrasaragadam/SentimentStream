import requests
from bs4 import BeautifulSoup
from airflow.models import BaseOperator
from datetime import datetime
import logging
import time

class FetchArticlesOperator(BaseOperator):
    def __init__(self, sources=None, keywords=None, max_results=5, *args, **kwargs):
        super(FetchArticlesOperator, self).__init__(*args, **kwargs)
        self.sources = sources
        self.keywords = keywords
        self.max_results = max_results

    def fetch_finshots_articles(self, keyword):
        articles = []
        page = 1
        while len(articles) < self.max_results:
            url = f"https://finshots.in/archive/page/{page}/"
            try:
                response = requests.get(url)
                response.raise_for_status() 
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
            time.sleep(1)  # Rate limiting
        return articles

    def fetch_yourstory_articles(self, keyword):
        articles = []
        url = f"https://yourstory.com/api/v2/tag/stories?slug={keyword.lower().replace(' ','-')}"

        try:
            response = requests.get(url)
            response.raise_for_status()
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
