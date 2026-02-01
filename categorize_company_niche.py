"""
Categorize Company Niche Script
Uses AI to analyze company content and determine the PRIMARY email-campaign-ready niche.
Uses OpenRouter (GPT-4o-mini) for cost-effective categorization.
"""

import os
import re
import requests
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# API Configuration
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
DEFAULT_MODEL = os.getenv('CATEGORIZE_NICHE_MODEL', 'openai/gpt-4o-mini')


@dataclass
class NicheResult:
    niche: str
    confidence: str  # High, Medium, Low
    reasoning: str
    success: bool
    error: Optional[str] = None


CATEGORIZATION_PROMPT = """You are an expert at categorizing companies for targeted B2B email campaigns.

Analyze the following company information and determine their PRIMARY niche. Even if the company serves multiple markets, identify the ONE niche they primarily focus on.

COMPANY INFORMATION:
{content}

INSTRUCTIONS:
1. Identify what the company primarily does
2. Determine their main target market (B2B, B2C, Enterprise, SMB, etc.)
3. Identify the specific industry vertical
4. Include a sub-specialty if clearly identifiable

OUTPUT FORMAT (respond EXACTLY in this format):
NICHE: [Business Model] - [Industry] - [Sub-specialty]
CONFIDENCE: [High/Medium/Low]
REASONING: [One sentence explaining why this is their primary niche]

EXAMPLE OUTPUTS:
- NICHE: B2B SaaS - HR Tech - Recruiting
- NICHE: Fintech - Payment Processing
- NICHE: E-commerce - Fashion - Sustainable
- NICHE: Healthcare - Telemedicine
- NICHE: Marketing Agency - Performance Marketing
- NICHE: Manufacturing - Industrial Equipment - HVAC
- NICHE: B2B Services - IT Consulting
- NICHE: Logistics - 3PL Fulfillment
- NICHE: Real Estate - Commercial Property Management

RULES:
- Always pick ONE primary niche, not multiple
- Be specific enough for targeted email campaigns
- Use industry-standard terminology
- If content is too vague, use "General Business" with Low confidence
- Format: 2-4 parts separated by " - "

Analyze now:"""


def categorize_niche(content: str, company_name: str = "") -> NicheResult:
    """
    Analyze company content and determine the primary niche.

    Args:
        content: Text content from website scraping or research
        company_name: Optional company name for context

    Returns:
        NicheResult with categorization
    """
    if not content or not content.strip():
        return NicheResult(
            niche="Insufficient Data",
            confidence="Low",
            reasoning="No content available for analysis",
            success=False,
            error="Empty content"
        )

    if not OPENROUTER_API_KEY:
        return NicheResult(
            niche="Error",
            confidence="Low",
            reasoning="API key not configured",
            success=False,
            error="OPENROUTER_API_KEY not set"
        )

    # Prepare content with company name if available
    full_content = content
    if company_name:
        full_content = f"Company Name: {company_name}\n\n{content}"

    # Truncate if too long
    if len(full_content) > 6000:
        full_content = full_content[:6000] + "..."

    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEFAULT_MODEL,
                "max_tokens": 300,
                "messages": [{
                    "role": "user",
                    "content": CATEGORIZATION_PROMPT.format(content=full_content)
                }]
            },
            timeout=30
        )

        response.raise_for_status()
        data = response.json()
        response_text = data['choices'][0]['message']['content']

        # Parse response
        niche = "Unknown"
        confidence = "Low"
        reasoning = ""

        niche_match = re.search(r'NICHE:\s*(.+?)(?=\n|CONFIDENCE:|$)', response_text, re.IGNORECASE)
        if niche_match:
            niche = niche_match.group(1).strip()

        confidence_match = re.search(r'CONFIDENCE:\s*(High|Medium|Low)', response_text, re.IGNORECASE)
        if confidence_match:
            confidence = confidence_match.group(1).capitalize()

        reasoning_match = re.search(r'REASONING:\s*(.+?)$', response_text, re.IGNORECASE | re.DOTALL)
        if reasoning_match:
            reasoning = reasoning_match.group(1).strip()

        # Validate niche format
        if niche and niche not in ["Unknown", "Error", "Insufficient Data"]:
            return NicheResult(
                niche=niche,
                confidence=confidence,
                reasoning=reasoning,
                success=True
            )
        else:
            return NicheResult(
                niche="General Business",
                confidence="Low",
                reasoning="Could not determine specific niche from available content",
                success=True  # Still considered success, just low confidence
            )

    except Exception as e:
        return NicheResult(
            niche="Error",
            confidence="Low",
            reasoning=str(e),
            success=False,
            error=f"API error: {str(e)}"
        )


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage: python categorize_company_niche.py <content_or_file>")
        print("       Pass content directly or path to a text file")
        sys.exit(1)

    arg = sys.argv[1]

    # Check if argument is a file path
    if os.path.isfile(arg):
        with open(arg, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        content = " ".join(sys.argv[1:])

    result = categorize_niche(content)

    print(f"Niche: {result.niche}")
    print(f"Confidence: {result.confidence}")
    print(f"Reasoning: {result.reasoning}")
    print(f"Success: {result.success}")
    if result.error:
        print(f"Error: {result.error}")
