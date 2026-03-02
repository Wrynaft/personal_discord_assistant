import re
from ddgs import DDGS


# Patterns that suggest the user is asking a factual question
_QUESTION_STARTERS = re.compile(
    r"^\s*(who|what|when|where|why|how|is|are|does|do|did|can|could|will|would|should|tell me about)\b",
    re.IGNORECASE,
)
_RECENCY_KEYWORDS = re.compile(
    r"\b(latest|current|recent|today|now|new|update|news|2024|2025|2026)\b",
    re.IGNORECASE,
)


def should_search(text: str) -> bool:
    """
    Lightweight heuristic to decide if a message likely needs web search.
    Returns True for factual questions or requests about recent/current info.
    """
    text = text.strip()
    if not text:
        return False

    # Explicit question mark
    if "?" in text:
        return True

    # Starts with a question word
    if _QUESTION_STARTERS.search(text):
        return True

    # Mentions something that implies needing current info
    if _RECENCY_KEYWORDS.search(text):
        return True

    return False


async def search_web(query: str, max_results: int = 5) -> str:
    """
    Searches DuckDuckGo and returns formatted results for LLM context.

    Args:
        query: The search query.
        max_results: Maximum number of results to return.

    Returns:
        A formatted string of search results, or empty string if no results.
    """
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))

        if not results:
            return ""

        formatted = "Web Search Results:\n\n"
        for i, r in enumerate(results, 1):
            title = r.get("title", "No title")
            body = r.get("body", "No description")
            url = r.get("href", "")
            formatted += f"{i}. **{title}**\n   {body}\n   Source: {url}\n\n"

        return formatted

    except Exception as e:
        print(f"Search error: {e}")
        return ""
