import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
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
            time.sleep(1)  # Rate limiting
        return articles

    def fetch_yourstory_articles(self, keyword):
        articles = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                url = f"https://yourstory.com/search?q={keyword}"
                page.goto(url)

                # Wait for articles to load
                page.wait_for_timeout(5000)  # Wait for 5 seconds

                # Find article elements
                article_elements = page.query_selector_all("article")
                for article in article_elements[:self.max_results]:
                    try:
                        title_tag = article.query_selector('h2')
                        link_tag = article.query_selector('a')
                        date_tag = article.query_selector('time')
                        description_tag = article.query_selector('p')

                        title = title_tag.inner_text() if title_tag else "No title"
                        link = link_tag.get_attribute('href') if link_tag else ""
                        date = date_tag.get_attribute('datetime') if date_tag else datetime.now().isoformat()
                        description = description_tag.inner_text() if description_tag else "No description available"

                        articles.append({
                            'title': title,
                            'link': link,
                            'keyword': keyword,
                            'date': date,
                            'description': description,
                            'source': 'YourStory',
                            'run_date': datetime.now().isoformat()
                        })

                        if len(articles) >= self.max_results:
                            break
                    except Exception as e:
                        self.log.error(f"Error processing article: {e}")

                browser.close()  # Close the browser
        except Exception as e:
            self.log.error(f"Playwright error: {e}")  # Log error without failing the job

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
