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

    async def autocomplete_tag(self, query, limit=5):
        """
        Use Danbooru's autocomplete API to fuzzy-match a tag.
        
        Args:
            query: Partial tag string (e.g., "hatsu", "genshn")
            limit: Max suggestions to return
        Returns:
            list of dicts with keys: name, post_count
        """
        await self._ensure_session()

        params = {
            "search[query]": query,
            "search[type]": "tag_query",
            "limit": limit,
        }

        try:
            async with self.session.get(f"{BASE_URL}/autocomplete.json", params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return [
                    {"name": item.get("value", ""), "post_count": item.get("post_count", 0)}
                    for item in data
                    if item.get("value")
                ]
        except Exception as e:
            print(f"Danbooru autocomplete error: {e}")
            return []

    async def resolve_tags(self, user_input):
        """
        Resolve user-typed tags to valid Danbooru tags using autocomplete.

        Args:
            user_input: Raw tag string from user (e.g., "hatsu genshn")
        Returns:
            (resolved_tags: str, corrections: list of (original, corrected) tuples)
        """
        tags = user_input.strip().replace(",", " ").split()
        resolved = []
        corrections = []

        for tag in tags[:2]:  # Max 2 tags (free tier)
            # Normalize: spaces to underscores
            tag = tag.strip().replace(" ", "_").lower()

            # Try autocomplete
            suggestions = await self.autocomplete_tag(tag, limit=1)

            if suggestions:
                best = suggestions[0]["name"]
                if best.lower() != tag.lower():
                    corrections.append((tag, best))
                resolved.append(best)
            else:
                # No match — use as-is
                resolved.append(tag)

        return " ".join(resolved), corrections

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
