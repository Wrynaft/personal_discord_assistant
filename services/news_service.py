import aiohttp
import config

class NewsService:
    def __init__(self):
        self.api_key = config.NEWS_API_KEY
        self.base_url = "https://newsapi.org/v2/top-headlines"

    async def fetch_tech_news(self, limit=10):
        """
        Fetches top technology headlines.
        Returns:
            list[dict] on success — each dict has: title, source, description, url
            str on error — error message string
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

                    # Return structured data for richer formatting
                    return [
                        {
                            "title": art.get("title", "No Title"),
                            "source": art.get("source", {}).get("name", "Unknown"),
                            "description": art.get("description") or "No description available.",
                            "url": art.get("url", ""),
                        }
                        for art in articles
                    ]
            except Exception as e:
                return f"Error fetching news: {e}"

    @staticmethod
    def format_for_llm(articles):
        """Formats the structured article list into a text block for the LLM."""
        text = "Here are today's top tech headlines:\n\n"
        for i, art in enumerate(articles, 1):
            text += f"{i}. [{art['source']}] {art['title']}\n"
            text += f"   Summary: {art['description']}\n"
            text += f"   Link: {art['url']}\n\n"
        return text
