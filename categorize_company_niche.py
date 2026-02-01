"""
Categorize Company Niche Script
Uses AI to analyze company content and determine the PRIMARY email-campaign-ready niche.
Uses OpenRouter (GPT-4o-mini) for cost-effective categorization.

Supports two modes:
- Single: One company at a time (high accuracy)
- Batch: 20 companies per API call (faster for large lists)
"""

import os
import re
import json
import requests
from typing import Optional, List, Dict
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Default model
DEFAULT_MODEL = os.getenv('CATEGORIZE_NICHE_MODEL', 'openai/gpt-4o-mini')


def get_api_key():
    """Get API key from Streamlit secrets or environment."""
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'OPENROUTER_API_KEY' in st.secrets:
            return st.secrets['OPENROUTER_API_KEY']
    except:
        pass
    return os.getenv('OPENROUTER_API_KEY')


# Get API key (supports both .env and Streamlit secrets)
OPENROUTER_API_KEY = get_api_key()


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


# Batch prompt for CLASSIFY mode (predefined niches)
BATCH_CLASSIFY_PROMPT = """You are an expert at categorizing companies for B2B email campaigns.

Classify each company below into ONE of the provided target niches.
Use fuzzy matching - if a company is similar to a niche, assign it (e.g., "Digital Marketing" matches "Marketing Agency").
If NO niche fits, respond with "Other - [your suggested niche]".

TARGET NICHES:
{niches_list}

COMPANIES TO CLASSIFY:
{companies_list}

OUTPUT FORMAT - Return ONLY a JSON array, no other text:
[
  {{"index": 1, "company": "Company Name", "niche": "Assigned Niche", "match_type": "exact|fuzzy|other"}},
  {{"index": 2, "company": "Company Name", "niche": "Other - Manufacturing", "match_type": "other"}}
]

RULES:
- Classify EVERY company - do not skip any
- Use fuzzy matching: "Digital Marketing Agency" should match "Marketing Agency"
- If no match, use "Other - [AI-suggested niche]" with match_type "other"
- match_type: "exact" (exact match), "fuzzy" (similar match), "other" (new category)
- Return ONLY valid JSON, no explanations"""


# Batch prompt for DISCOVER mode (AI decides niches)
BATCH_DISCOVER_PROMPT = """You are an expert at categorizing companies for B2B email campaigns.

Categorize each company into a niche suitable for targeted email outreach.
Group similar companies together - normalize variations (e.g., "Digital Marketing" and "Online Marketing" become "Marketing Agency").

COMPANIES TO CATEGORIZE:
{companies_list}

OUTPUT FORMAT - Return ONLY a JSON array, no other text:
[
  {{"index": 1, "company": "Company Name", "niche": "Marketing Agency"}},
  {{"index": 2, "company": "Company Name", "niche": "B2B SaaS - HR Tech"}}
]

NICHE FORMAT: Use "[Business Model] - [Industry]" or "[Industry] - [Specialty]"
Examples: "Marketing Agency", "B2B SaaS - HR Tech", "E-commerce - Fashion", "Healthcare - Telemedicine"

RULES:
- Categorize EVERY company - do not skip any
- Normalize similar niches (don't create 50 variations of "Marketing")
- Be specific enough for email targeting but not overly granular
- Return ONLY valid JSON, no explanations"""


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


def categorize_niche_batch(
    companies: List[Dict[str, str]],
    predefined_niches: List[str] = None,
    batch_size: int = 20
) -> List[Dict]:
    """
    Batch categorize companies into niches.

    Args:
        companies: List of dicts with 'name' and optionally 'content' keys
        predefined_niches: List of target niches (None = Discover mode)
        batch_size: Number of companies per API call (default 20)

    Returns:
        List of dicts with 'index', 'company', 'niche', 'match_type'
    """
    api_key = get_api_key()
    if not api_key:
        return [{"index": i, "company": c.get("name", ""), "niche": "Error", "match_type": "error", "error": "API key not configured"}
                for i, c in enumerate(companies)]

    all_results = []

    # Process in batches
    for batch_start in range(0, len(companies), batch_size):
        batch = companies[batch_start:batch_start + batch_size]

        # Format companies for prompt
        companies_text = "\n".join([
            f"{i + 1}. {c.get('name', 'Unknown')} | {c.get('content', c.get('name', ''))[:100]}"
            for i, c in enumerate(batch)
        ])

        # Choose prompt based on mode
        if predefined_niches:
            niches_text = "\n".join([f"- {n}" for n in predefined_niches])
            prompt = BATCH_CLASSIFY_PROMPT.format(
                niches_list=niches_text,
                companies_list=companies_text
            )
        else:
            prompt = BATCH_DISCOVER_PROMPT.format(companies_list=companies_text)

        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": DEFAULT_MODEL,
                    "max_tokens": 2000,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=60
            )

            response.raise_for_status()
            data = response.json()
            response_text = data['choices'][0]['message']['content']

            # Extract JSON from response
            json_match = re.search(r'\[[\s\S]*\]', response_text)
            if json_match:
                batch_results = json.loads(json_match.group())

                # Adjust indices to global position
                for result in batch_results:
                    original_index = result.get("index", 1) - 1  # Convert to 0-based
                    global_index = batch_start + original_index

                    if 0 <= original_index < len(batch):
                        all_results.append({
                            "index": global_index,
                            "company": batch[original_index].get("name", ""),
                            "niche": result.get("niche", "Unknown"),
                            "match_type": result.get("match_type", "unknown")
                        })
            else:
                # Failed to parse - add error results for batch
                for i, c in enumerate(batch):
                    all_results.append({
                        "index": batch_start + i,
                        "company": c.get("name", ""),
                        "niche": "Parse Error",
                        "match_type": "error"
                    })

        except Exception as e:
            # API error - add error results for batch
            for i, c in enumerate(batch):
                all_results.append({
                    "index": batch_start + i,
                    "company": c.get("name", ""),
                    "niche": "API Error",
                    "match_type": "error",
                    "error": str(e)
                })

    # Sort by index to maintain order
    all_results.sort(key=lambda x: x.get("index", 0))

    return all_results


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
