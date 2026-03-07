"""
Danbooru Service — Fetches top-rated posts from Danbooru's API.

API docs: https://danbooru.donmai.us/wiki_pages/help:api
- Free tier: no auth needed, 10 req/s, 2 tags per search, 100 results max
"""

import aiohttp
from datetime import datetime, timezone, timedelta

BASE_URL = "https://danbooru.donmai.us"
MYT = timezone(timedelta(hours=8))


class DanbooruService:
    def __init__(self):
        self.session = None

    async def _ensure_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def get_top_posts(self, tags="", limit=5, date=None):
        """
        Fetch top-rated posts from Danbooru.

        Args:
            tags: Space-separated tags (max 2 for free tier).
            limit: Number of posts to return (max 100).
            date: Date string (YYYY-MM-DD) to filter posts from. Defaults to today.
        Returns:
            list of post dicts with keys: id, url, score, tags, source, rating, file_url, preview_url
        """
        await self._ensure_session()

        if not date:
            date = datetime.now(MYT).strftime("%Y-%m-%d")

        # Build tag query — add date and score sort
        tag_query = f"date:{date} order:score"
        if tags:
            # Free tier: 2 tag limit. date: and order: are meta-tags and don't count.
            tag_query = f"{tags} {tag_query}"

        params = {
            "tags": tag_query,
            "limit": limit,
        }

        try:
            async with self.session.get(f"{BASE_URL}/posts.json", params=params) as resp:
                if resp.status != 200:
                    print(f"Danbooru API error: {resp.status}")
                    return []

                data = await resp.json()

                posts = []
                for post in data:
                    # Skip posts without a viewable image
                    file_url = post.get("file_url") or post.get("large_file_url")
                    if not file_url:
                        continue

                    posts.append({
                        "id": post["id"],
                        "score": post.get("score", 0),
                        "rating": post.get("rating", "g"),  # g=general, s=sensitive, q=questionable, e=explicit
                        "tags": post.get("tag_string_general", "")[:200],
                        "artist": post.get("tag_string_artist", "Unknown"),
                        "character": post.get("tag_string_character", ""),
                        "copyright": post.get("tag_string_copyright", ""),
                        "source": post.get("source", ""),
                        "file_url": file_url,
                        "preview_url": post.get("preview_file_url", file_url),
                        "page_url": f"{BASE_URL}/posts/{post['id']}",
                        "width": post.get("image_width", 0),
                        "height": post.get("image_height", 0),
                    })

                return posts[:limit]

        except Exception as e:
            print(f"Error fetching Danbooru posts: {e}")
            return []

    async def get_random_top_post(self, tags=""):
        """Fetch a single random post from today's top 20."""
        import random
        posts = await self.get_top_posts(tags=tags, limit=20)
        if not posts:
            # Fallback: try yesterday's posts
            yesterday = (datetime.now(MYT) - timedelta(days=1)).strftime("%Y-%m-%d")
            posts = await self.get_top_posts(tags=tags, limit=20, date=yesterday)
        return random.choice(posts) if posts else None

    @staticmethod
    def rating_emoji(rating):
        """Convert rating code to emoji."""
        return {
            "g": "🟢 General",
            "s": "🟡 Sensitive",
            "q": "🟠 Questionable",
            "e": "🔴 Explicit",
        }.get(rating, "⚪ Unknown")
