"""
Scrape Homepage Script
Fetches and extracts content from company websites for niche categorization.
"""

import re
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict
from dataclasses import dataclass

TIMEOUT = 15
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


@dataclass
class ScrapedContent:
    url: str
    title: str
    meta_description: str
    headings: str
    main_content: str
    success: bool
    error: Optional[str] = None

    def to_text(self) -> str:
        """Combine all content into a single text for analysis."""
        parts = []
        if self.title:
            parts.append(f"Title: {self.title}")
        if self.meta_description:
            parts.append(f"Description: {self.meta_description}")
        if self.headings:
            parts.append(f"Headings: {self.headings}")
        if self.main_content:
            parts.append(f"Content: {self.main_content[:3000]}")  # Limit content length
        return "\n".join(parts)


def normalize_url(url: str) -> str:
    """Ensure URL has protocol and is properly formatted."""
    url = url.strip()
    if not url:
        return ""

    # Remove common prefixes that aren't protocols
    url = re.sub(r'^(www\.)', '', url, flags=re.IGNORECASE)

    # Add https if no protocol
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    # Remove trailing slash for consistency
    url = url.rstrip('/')

    return url


def fetch_page(url: str) -> Optional[str]:
    """Fetch a webpage and return its HTML content."""
    try:
        url = normalize_url(url)
        if not url:
            return None

        response = requests.get(
            url,
            headers=HEADERS,
            timeout=TIMEOUT,
            allow_redirects=True,
            verify=True
        )
        response.raise_for_status()
        return response.text
    except requests.exceptions.SSLError:
        # Try without SSL verification as fallback
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True, verify=False)
            response.raise_for_status()
            return response.text
        except:
            return None
    except Exception as e:
        return None


def extract_title(soup: BeautifulSoup) -> str:
    """Extract page title."""
    title_tag = soup.find('title')
    if title_tag:
        return title_tag.get_text(strip=True)

    # Fallback to og:title
    og_title = soup.find('meta', property='og:title')
    if og_title:
        return og_title.get('content', '')

    return ""


def extract_meta_description(soup: BeautifulSoup) -> str:
    """Extract meta description."""
    # Standard meta description
    meta = soup.find('meta', attrs={'name': 'description'})
    if meta:
        return meta.get('content', '')

    # OpenGraph description
    og_desc = soup.find('meta', property='og:description')
    if og_desc:
        return og_desc.get('content', '')

    return ""


def extract_headings(soup: BeautifulSoup) -> str:
    """Extract h1, h2, h3 headings."""
    headings = []
    for tag in ['h1', 'h2', 'h3']:
        for heading in soup.find_all(tag):
            text = heading.get_text(strip=True)
            if text and len(text) > 2:
                headings.append(text)

    return " | ".join(headings[:15])  # Limit to first 15 headings


def extract_main_content(soup: BeautifulSoup) -> str:
    """Extract main text content from the page."""
    # Remove unwanted elements
    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'form', 'noscript']):
        tag.decompose()

    # Try to find main content area
    main = soup.find('main') or soup.find('article') or soup.find('div', {'id': 'content'}) or soup.find('div', {'class': 'content'})

    if main:
        text = main.get_text(separator=' ', strip=True)
    else:
        text = soup.get_text(separator=' ', strip=True)

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)

    return text


def scrape_homepage(url: str) -> ScrapedContent:
    """
    Scrape a company homepage and extract relevant content.

    Args:
        url: The website URL to scrape

    Returns:
        ScrapedContent with extracted information
    """
    normalized_url = normalize_url(url)

    if not normalized_url:
        return ScrapedContent(
            url=url,
            title="",
            meta_description="",
            headings="",
            main_content="",
            success=False,
            error="Invalid or empty URL"
        )

    html = fetch_page(normalized_url)

    if not html:
        return ScrapedContent(
            url=normalized_url,
            title="",
            meta_description="",
            headings="",
            main_content="",
            success=False,
            error="Failed to fetch page"
        )

    try:
        soup = BeautifulSoup(html, 'html.parser')

        return ScrapedContent(
            url=normalized_url,
            title=extract_title(soup),
            meta_description=extract_meta_description(soup),
            headings=extract_headings(soup),
            main_content=extract_main_content(soup),
            success=True
        )
    except Exception as e:
        return ScrapedContent(
            url=normalized_url,
            title="",
            meta_description="",
            headings="",
            main_content="",
            success=False,
            error=f"Parse error: {str(e)}"
        )


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage: python scrape_homepage.py <url>")
        sys.exit(1)

    url = sys.argv[1]
    result = scrape_homepage(url)

    print(f"URL: {result.url}")
    print(f"Success: {result.success}")
    if result.error:
        print(f"Error: {result.error}")
    else:
        print(f"Title: {result.title}")
        print(f"Description: {result.meta_description}")
        print(f"Headings: {result.headings[:200]}...")
        print(f"Content preview: {result.main_content[:500]}...")
