import aiohttp
import config

class NewsService:
    def __init__(self):
        self.api_key = config.NEWS_API_KEY
        self.base_url = "https://newsapi.org/v2/top-headlines"

    async def fetch_tech_news(self, limit=10):
        """
        Fetches top technology headlines.
        Returns: String summary of headlines for the LLM.
        """
        if not self.api_key:
            return "Error: NEWS_API_KEY is missing."

        params = {
            "category": "technology",
            "language": "en",
            "pageSize": limit,
            "apiKey": self.api_key
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.base_url, params=params) as response:
                    if response.status != 200:
                        return f"Error fetching news: HTTP {response.status}"
                    
                    data = await response.json()
                    articles = data.get("articles", [])
                    
                    if not articles:
                        return "No news found today."

                    # Format for LLM
                    news_text = "Here are the top tech headlines for today:\n"
                    for i, art in enumerate(articles, 1):
                        source = art.get("source", {}).get("name", "Unknown")
                        title = art.get("title", "No Title")
                        desc = art.get("description") or "No description"
                        url = art.get("url", "")
                        news_text += f"{i}. [{source}] {title}\n   Context: {desc}\n   Link: {url}\n"
                    
                    return news_text
            except Exception as e:
                return f"Error fetching news: {e}"
