import aiohttp
import xml.etree.ElementTree as ET


# arXiv Atom XML namespace
ATOM_NS = "{http://www.w3.org/2005/Atom}"
ARXIV_NS = "{http://arxiv.org/schemas/atom}"

# CS categories relevant to a CS/DS student
DEFAULT_CATEGORIES = ["cs.AI", "cs.LG", "cs.CL", "cs.CV"]


class ArxivService:
    """Fetches recent research papers from arXiv."""

    BASE_URL = "http://export.arxiv.org/api/query"

    async def fetch_recent_papers(self, categories=None, limit=8):
        """
        Fetches recent papers from arXiv in the specified CS categories.

        Args:
            categories: List of arXiv category codes (e.g. ["cs.AI", "cs.LG"]).
            limit: Maximum number of papers to return.

        Returns:
            list[dict] on success — each dict has: title, authors, abstract, url, categories
            str on error — error message string
        """
        cats = categories or DEFAULT_CATEGORIES
        cat_query = "+OR+".join(f"cat:{c}" for c in cats)

        params = {
            "search_query": cat_query,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "max_results": limit,
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.BASE_URL, params=params) as response:
                    if response.status != 200:
                        return f"Error fetching arXiv papers: HTTP {response.status}"

                    xml_text = await response.text()
                    return self._parse_feed(xml_text)
            except Exception as e:
                return f"Error fetching arXiv papers: {e}"

    def _parse_feed(self, xml_text):
        """Parses the arXiv Atom XML feed into structured data."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            return f"Error parsing arXiv response: {e}"

        entries = root.findall(f"{ATOM_NS}entry")
        if not entries:
            return "No recent papers found."

        papers = []
        for entry in entries:
            title = entry.findtext(f"{ATOM_NS}title", "No Title").strip().replace("\n", " ")

            # Get authors (may have multiple)
            authors = [
                a.findtext(f"{ATOM_NS}name", "Unknown")
                for a in entry.findall(f"{ATOM_NS}author")
            ]
            # Truncate long author lists
            if len(authors) > 3:
                author_str = f"{', '.join(authors[:3])} et al."
            else:
                author_str = ", ".join(authors)

            abstract = entry.findtext(f"{ATOM_NS}summary", "No abstract.").strip().replace("\n", " ")
            # Truncate long abstracts for token efficiency
            if len(abstract) > 300:
                abstract = abstract[:297] + "..."

            # Get the paper URL (the abs link)
            url = ""
            for link in entry.findall(f"{ATOM_NS}link"):
                if link.get("type") == "text/html":
                    url = link.get("href", "")
                    break
            if not url:
                url = entry.findtext(f"{ATOM_NS}id", "")

            # Get categories
            categories = [
                cat.get("term", "")
                for cat in entry.findall(f"{ATOM_NS}category")
                if cat.get("term", "").startswith("cs.")
            ]

            papers.append({
                "title": title,
                "authors": author_str,
                "abstract": abstract,
                "url": url,
                "categories": ", ".join(categories[:3]),
            })

        return papers

    @staticmethod
    def format_for_llm(papers):
        """Formats the structured paper list into a text block for the LLM."""
        text = "Here are the latest research papers from arXiv:\n\n"
        for i, p in enumerate(papers, 1):
            text += f"{i}. \"{p['title']}\"\n"
            text += f"   Authors: {p['authors']}\n"
            text += f"   Categories: {p['categories']}\n"
            text += f"   Abstract: {p['abstract']}\n"
            text += f"   Link: {p['url']}\n\n"
        return text
