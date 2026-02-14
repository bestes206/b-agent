"""Web search tool — fetches and summarizes web pages."""

import requests
from bs4 import BeautifulSoup


def search_web(query: str, num_results: int = 3) -> str:
    """Search the web using DuckDuckGo's HTML interface and return results.

    Args:
        query: The search query string.
        num_results: How many results to return (default 3).

    Returns:
        A formatted string of search results with titles, URLs, and snippets.
    """
    url = "https://html.duckduckgo.com/html/"
    headers = {"User-Agent": "Mozilla/5.0 (b-agent/1.0)"}

    try:
        response = requests.post(url, data={"q": query}, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        return f"Search failed: {e}"

    soup = BeautifulSoup(response.text, "html.parser")
    results = []

    for item in soup.select(".result")[:num_results]:
        title_tag = item.select_one(".result__title a")
        snippet_tag = item.select_one(".result__snippet")
        if title_tag:
            title = title_tag.get_text(strip=True)
            link = title_tag.get("href", "")
            snippet = snippet_tag.get_text(strip=True) if snippet_tag else ""
            results.append(f"- {title}\n  URL: {link}\n  {snippet}")

    return "\n\n".join(results) if results else "No results found."


def fetch_page(url: str, max_chars: int = 3000) -> str:
    """Fetch a web page and return its text content (truncated).

    Args:
        url: The page URL to fetch.
        max_chars: Maximum characters to return (default 3000).

    Returns:
        The plain-text content of the page, truncated to max_chars.
    """
    headers = {"User-Agent": "Mozilla/5.0 (b-agent/1.0)"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        return f"Failed to fetch page: {e}"

    soup = BeautifulSoup(response.text, "html.parser")

    # Remove scripts and styles
    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n... [truncated]"
    return text
