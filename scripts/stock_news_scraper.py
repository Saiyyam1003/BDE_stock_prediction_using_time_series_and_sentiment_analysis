import requests
from datetime import datetime, timedelta
import json

class StockNewsScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
        self.headers = {
            "X-Api-Key": self.api_key
        }

    def fetch_stock_news(self, companies, days_back=7, language="en", sort_by="publishedAt", page_size=10):
        """
        Fetch recent stock news for specific companies
        
        Args:
            companies (list): List of company names or symbols (e.g., ["AAPL", "Tesla"])
            days_back (int): How many days back to fetch news
            language (str): Language of news articles
            sort_by (str): Sort order ('relevancy', 'popularity', 'publishedAt')
            page_size (int): Number of results to return per company
            
        Returns:
            dict: Dictionary with company-specific news articles
        """
        # Calculate the date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        all_company_news = {}
        
        for company in companies:
            # Create query with company name/symbol and common stock-related terms
            query = f'"{company}" AND (stock OR shares OR market OR trading OR earnings OR investor OR financial)'
            
            params = {
                "q": query,
                "from": start_date.strftime("%Y-%m-%d"),
                "to": end_date.strftime("%Y-%m-%d"),
                "language": language,
                "sortBy": sort_by,
                "pageSize": page_size
            }

            try:
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                
                if data["status"] != "ok":
                    raise Exception(f"API Error for {company}: {data.get('message', 'Unknown error')}")
                
                # Process and clean the articles
                processed_articles = []
                for article in data["articles"]:
                    processed_article = {
                        "title": article.get("title"),
                        "description": article.get("description"),
                        "source": article.get("source", {}).get("name"),
                        "url": article.get("url"),
                        "published_at": article.get("publishedAt"),
                        "fetch_timestamp": datetime.utcnow().isoformat(),
                        "company": company
                    }
                    processed_articles.append(processed_article)
                
                all_company_news[company] = processed_articles
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching news for {company}: {str(e)}")
                all_company_news[company] = []
                continue
                
        return all_company_news

def main():
    # Initialize the scraper with your API key
    api_key = "b5866a249cb0499c9aa3a7eb82e8f7f9"
    scraper = StockNewsScraper(api_key)
    
    # List of companies you want to track
    companies = ["AAPL", "TSLA", "MSFT", "GOOGL"]
    
    try:
        # Get recent stock news for the companies
        stock_news = scraper.fetch_stock_news(
            companies=companies,
            days_back=7,  # Get news from last 3 days
            page_size=10   # Get 5 articles per company
        )
        
        # Print the results in a readable format
        for company, articles in stock_news.items():
            print(f"\nNews for {company}:")
            print("=" * 50)
            
            if not articles:
                print(f"No news found for {company}")
                continue
                
            for article in articles:
                print(f"Title: {article['title']}")
                print(f"Source: {article['source']}")
                print(f"Published: {article['published_at']}")
                print(f"URL: {article['url']}")
                print("-" * 50)
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()