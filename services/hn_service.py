import aiohttp


class HNService:
    """Fetches top stories from Hacker News via the Algolia API."""

    ALGOLIA_URL = "http://hn.algolia.com/api/v1/search"

    async def fetch_top_stories(self, limit=10):
        """
        Fetches the current front page stories from Hacker News.

        Returns:
            list[dict] on success — each dict has: title, url, points, comments, hn_url
            str on error — error message string
        """
        params = {
            "tags": "front_page",
            "hitsPerPage": limit,
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.ALGOLIA_URL, params=params) as response:
                    if response.status != 200:
                        return f"Error fetching Hacker News: HTTP {response.status}"

                    data = await response.json()
                    hits = data.get("hits", [])

                    if not hits:
                        return "No Hacker News stories found."

                    return [
                        {
                            "title": hit.get("title", "No Title"),
                            "url": hit.get("url") or f"https://news.ycombinator.com/item?id={hit['objectID']}",
                            "points": hit.get("points", 0),
                            "comments": hit.get("num_comments", 0),
                            "hn_url": f"https://news.ycombinator.com/item?id={hit['objectID']}",
                        }
                        for hit in hits
                    ]
            except Exception as e:
                return f"Error fetching Hacker News: {e}"

    @staticmethod
    def format_for_llm(stories):
        """Formats the structured story list into a text block for the LLM."""
        text = "Here are today's top Hacker News stories:\n\n"
        for i, story in enumerate(stories, 1):
            text += f"{i}. {story['title']}\n"
            text += f"   Points: {story['points']} | Comments: {story['comments']}\n"
            text += f"   Article: {story['url']}\n"
            text += f"   Discussion: {story['hn_url']}\n\n"
        return text
