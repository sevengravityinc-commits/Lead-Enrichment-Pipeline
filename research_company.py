"""
Research Company Script
Uses web search and AI to research companies when no website URL is available.
"""

import os
import re
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


@dataclass
class ResearchResult:
    company_name: str
    research_summary: str
    inferred_website: Optional[str]
    success: bool
    error: Optional[str] = None

    def to_text(self) -> str:
        """Return research summary for niche analysis."""
        return self.research_summary


def clean_company_name(name: str) -> str:
    """Clean company name for better search results."""
    if not name:
        return ""

    # Remove common suffixes
    suffixes = [
        r'\s+(Inc\.?|LLC|Ltd\.?|Corp\.?|Corporation|Company|Co\.?|Limited|GmbH|SA|AG|PLC)$',
        r'\s*,\s*(Inc\.?|LLC|Ltd\.?|Corp\.?)$',
    ]

    cleaned = name.strip()
    for suffix in suffixes:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)

    return cleaned.strip()


def research_with_claude(company_name: str) -> ResearchResult:
    """
    Use Claude to research a company and determine what they do.
    Claude will use its knowledge and web search capabilities.
    """
    if not ANTHROPIC_AVAILABLE:
        return ResearchResult(
            company_name=company_name,
            research_summary="",
            inferred_website=None,
            success=False,
            error="Anthropic library not installed"
        )

    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        return ResearchResult(
            company_name=company_name,
            research_summary="",
            inferred_website=None,
            success=False,
            error="ANTHROPIC_API_KEY not set"
        )

    client = Anthropic(api_key=api_key)

    prompt = f"""Research the company "{company_name}" and provide a brief summary of:
1. What industry/sector they operate in
2. What products or services they offer
3. Who their target customers are (B2B, B2C, enterprise, SMB, etc.)
4. Their primary business model

If you can find their website URL, include it.

Keep the response concise (3-5 sentences max). Focus on factual information about what the company does.
If you cannot find information about this company, say "Unable to find information about this company."

Format your response as:
SUMMARY: [your summary here]
WEBSITE: [website URL if found, or "Unknown"]"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text

        # Parse response
        summary = ""
        website = None

        summary_match = re.search(r'SUMMARY:\s*(.+?)(?=WEBSITE:|$)', response_text, re.DOTALL)
        if summary_match:
            summary = summary_match.group(1).strip()

        website_match = re.search(r'WEBSITE:\s*(\S+)', response_text)
        if website_match:
            url = website_match.group(1).strip()
            if url.lower() != "unknown" and url.startswith(('http', 'www.')):
                website = url

        if "Unable to find information" in response_text or not summary:
            return ResearchResult(
                company_name=company_name,
                research_summary=response_text,
                inferred_website=website,
                success=False,
                error="No information found"
            )

        return ResearchResult(
            company_name=company_name,
            research_summary=summary,
            inferred_website=website,
            success=True
        )

    except Exception as e:
        return ResearchResult(
            company_name=company_name,
            research_summary="",
            inferred_website=None,
            success=False,
            error=f"API error: {str(e)}"
        )


def research_company(company_name: str) -> ResearchResult:
    """
    Research a company when no website URL is available.

    Args:
        company_name: The name of the company to research

    Returns:
        ResearchResult with company information
    """
    if not company_name or not company_name.strip():
        return ResearchResult(
            company_name=company_name or "",
            research_summary="",
            inferred_website=None,
            success=False,
            error="No company name provided"
        )

    cleaned_name = clean_company_name(company_name)

    # Use Claude for research
    return research_with_claude(cleaned_name)


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage: python research_company.py <company_name>")
        sys.exit(1)

    company_name = " ".join(sys.argv[1:])
    result = research_company(company_name)

    print(f"Company: {result.company_name}")
    print(f"Success: {result.success}")
    if result.error:
        print(f"Error: {result.error}")
    if result.research_summary:
        print(f"Summary: {result.research_summary}")
    if result.inferred_website:
        print(f"Website: {result.inferred_website}")
