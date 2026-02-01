"""
Normalize Company Name Script
Uses Claude AI (via OpenRouter or Anthropic) to normalize company names for email personalization.
Based on user's n8n workflow logic.
"""

import os
import sys
import requests
from typing import Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Check which API is available
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

# Model configuration (can be overridden in .env)
# OpenRouter models: deepseek/deepseek-chat, anthropic/claude-3.5-haiku, openai/gpt-4o-mini, etc.
# Full list: https://openrouter.ai/models
DEFAULT_MODEL = os.getenv('NORMALIZER_MODEL', 'deepseek/deepseek-chat')

# Batch size for processing multiple names in one API call
BATCH_SIZE = int(os.getenv('NORMALIZER_BATCH_SIZE', '50'))

try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


@dataclass
class NormalizeResult:
    original: str
    normalized: str
    success: bool
    error: Optional[str] = None


NORMALIZATION_PROMPT = """# ROLE
You are an email personalization specialist preparing company names for cold outreach campaigns.

# TASK CONTEXT
This name will appear in email body copy like: "Hey {FirstName}, I noticed {CompanyName} is hiring..."

**Goal:** Make it sound natural in conversation while preserving brand identity.

# RULES

1. Remove legal suffixes: Ltd, LLC, Inc, Corp, Corporation, PLC, GmbH, Co, Company, Pty, Pte, AG, SA, SAS
2. Remove trailing generic terms: Solutions, Technologies, Software, Services, Consulting, Partners, Group, Holdings, Global, Digital, Media
3. Fix CamelCase: "BlueLogic" → "Blue Logic"
4. Preserve acronyms: "IBM Corporation" → "IBM" (not "Ibm")
5. Acronym + generic: "STD Data Solutions" → "STD"
6. Title case: "ACME SOLUTIONS" → "Acme"
7. Clean punctuation: Remove trailing punctuation, quotes, extra spaces

# QUALITY TEST
Would this sound natural spoken aloud?
- "I noticed Microsoft is hiring" ✅
- "I noticed Microsoft Corporation is hiring" ❌

Return ONLY the normalized name. No explanation.

Examples:
Input: Bluelogic LTD
Output: Blue Logic

Input: IBM Corporation
Output: IBM

Input: STD Data Solutions
Output: STD

Input: Data Solutions LLC
Output: Data Solutions

Input: ACME Incorporated
Output: Acme

Input: Insight Software
Output: Insight"""


BATCH_NORMALIZATION_PROMPT = """# ROLE
You are an email personalization specialist preparing company names for cold outreach campaigns.

# TASK CONTEXT
These names will appear in email body copy like:
- "Hey {FirstName}, I noticed {CompanyName} is hiring..."
- "Our clients at {CompanyName} typically see 3x ROI..."
- "Unlike other tools, we built specifically for {CompanyName}'s workflow..."

**Why this matters:**
- "Hey John, I noticed Acme Corporation Inc." → sounds robotic, kills reply rates
- "Hey John, I noticed Acme is hiring..." → natural, human, builds trust

**Your goal:** Make company names sound natural in conversation while preserving brand identity.

# NORMALIZATION RULES

## 1. Remove Legal Suffixes (ALWAYS)
Remove: Ltd, LLC, Inc, Corp, Corporation, PLC, GmbH, Co, Company, Pty, Pte, AG, SA, SAS, SRL, BV, NV
Examples:
- "Acme Corporation" → "Acme"
- "Blue Ocean Ltd" → "Blue Ocean"
- "Insight Software Inc." → "Insight Software"

## 2. Remove Generic Business Terms (ONLY when trailing)
Remove trailing: Solutions, Technologies, Technology, Software, Services, Consulting, Partners, Group, Holdings, International, Global, Enterprises, Industries, Systems, Labs, Digital, Media, Network, Analytics, Ventures, Capital
Examples:
- "Insight Software" → "Insight" ✅
- "Microsoft Corporation" → "Microsoft" ✅
- "Marketing Agency Inc" → "Marketing Agency" ✅ ("Agency" is part of brand)

## 3. Acronym Handling
Preserve acronyms - If company name is all-caps:
- "IBM Corporation" → "IBM"
- "STD Data Solutions" → "STD" (acronym + generic terms → keep only acronym)
- "AWS LLC" → "AWS"

## 4. CamelCase Splitting
Fix joined words:
- "BlueLogic" → "Blue Logic"
- "DataDog" → "Data Dog"

## 5. Capitalization
Use title case for multi-word names:
- "ACME SOLUTIONS" → "Acme"
- "data analytics group" → "Data Analytics"

Preserve acronyms:
- "IBM services" → "IBM" (not "Ibm")

## 6. Punctuation Cleanup
Remove:
- Trailing punctuation: "Acme, Inc." → "Acme"
- Quotes: '"Best Company"' → "Best Company"
- Extra spaces: "Acme    Corp" → "Acme"

Keep:
- Internal punctuation that's part of brand: "Procter & Gamble" → "Procter & Gamble"

# QUALITY CRITERIA

Good normalization:
✅ Sounds natural when spoken aloud
✅ Preserves brand recognition
✅ Removes legal/corporate jargon
✅ Would work in: "I saw that {CompanyName} just announced..."

Bad normalization:
❌ Too aggressive: "Microsoft Services Corporation" → "Microsoft Services" (should be "Microsoft")
❌ Breaks acronyms: "IBM" → "Ibm"

# EDGE CASES

Numerical prefixes:
- "3M Company" → "3M"
- "7-Eleven Inc" → "7-Eleven"

When in doubt:
- Choose the version that sounds more natural in conversation
- "I noticed Microsoft is hiring" ✅
- "I noticed Microsoft Corporation is hiring" ❌

# OUTPUT FORMAT
Return ONLY a JSON array of normalized names in the same order as input. No explanations.

Example:
Input: ["Bluelogic LTD", "IBM Corporation", "STD Data Solutions"]
Output: ["Blue Logic", "IBM", "STD"]"""


def normalize_batch_via_openrouter(company_names: list[str], model: str = None) -> list[NormalizeResult]:
    """Normalize multiple company names in a single API call."""
    import json

    model = model or DEFAULT_MODEL

    # Filter out empty names and track their positions
    valid_names = []
    valid_indices = []
    results = [None] * len(company_names)

    for i, name in enumerate(company_names):
        if name and name.strip():
            valid_names.append(name.strip())
            valid_indices.append(i)
        else:
            results[i] = NormalizeResult(
                original=name or "",
                normalized="",
                success=False,
                error="Empty company name"
            )

    if not valid_names:
        return results

    try:
        # Format names as JSON array for the prompt
        names_json = json.dumps(valid_names)

        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": len(valid_names) * 50,  # Estimate ~50 tokens per name
                "messages": [
                    {"role": "system", "content": BATCH_NORMALIZATION_PROMPT},
                    {"role": "user", "content": f"Normalize these company names:\n{names_json}"}
                ]
            },
            timeout=60
        )

        response.raise_for_status()
        data = response.json()
        response_text = data['choices'][0]['message']['content'].strip()

        # Parse JSON response
        # Handle cases where model might wrap in markdown code blocks
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
        response_text = response_text.strip()

        normalized_names = json.loads(response_text)

        # Map results back to original positions
        for i, idx in enumerate(valid_indices):
            original = valid_names[i]
            if i < len(normalized_names):
                normalized = str(normalized_names[i]).strip()
                # Clean up any extra formatting
                normalized = normalized.replace('"', '').replace("'", "").strip()
                # Sanity check
                if not normalized or len(normalized) > len(original) * 2:
                    normalized = original
                results[idx] = NormalizeResult(
                    original=original,
                    normalized=normalized,
                    success=True
                )
            else:
                results[idx] = NormalizeResult(
                    original=original,
                    normalized=original,
                    success=False,
                    error="Missing from API response"
                )

        return results

    except json.JSONDecodeError as e:
        # If JSON parsing fails, fall back to individual processing
        for i, idx in enumerate(valid_indices):
            results[idx] = NormalizeResult(
                original=valid_names[i],
                normalized=valid_names[i],
                success=False,
                error=f"JSON parse error: {str(e)}"
            )
        return results

    except Exception as e:
        # On any error, mark all as failed
        for i, idx in enumerate(valid_indices):
            results[idx] = NormalizeResult(
                original=valid_names[i],
                normalized=valid_names[i],
                success=False,
                error=f"OpenRouter API error: {str(e)}"
            )
        return results


def normalize_via_openrouter(company_name: str, model: str = None) -> NormalizeResult:
    """Normalize using OpenRouter API."""
    model = model or DEFAULT_MODEL
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 100,
                "messages": [
                    {"role": "system", "content": NORMALIZATION_PROMPT},
                    {"role": "user", "content": f"Normalize this company name:\n{company_name}"}
                ]
            },
            timeout=30
        )

        response.raise_for_status()
        data = response.json()
        normalized = data['choices'][0]['message']['content'].strip()

        # Clean up any extra formatting
        normalized = normalized.replace('"', '').replace("'", "").strip()

        # If the model returned something weird, fall back to original
        if not normalized or len(normalized) > len(company_name) * 2:
            normalized = company_name

        return NormalizeResult(
            original=company_name,
            normalized=normalized,
            success=True
        )

    except Exception as e:
        return NormalizeResult(
            original=company_name,
            normalized=company_name,
            success=False,
            error=f"OpenRouter API error: {str(e)}"
        )


def normalize_via_anthropic(company_name: str) -> NormalizeResult:
    """Normalize using Anthropic API directly."""
    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    try:
        response = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=100,
            system=NORMALIZATION_PROMPT,
            messages=[{
                "role": "user",
                "content": f"Normalize this company name:\n{company_name}"
            }]
        )

        normalized = response.content[0].text.strip()

        # Clean up any extra formatting
        normalized = normalized.replace('"', '').replace("'", "").strip()

        # If the model returned something weird, fall back to original
        if not normalized or len(normalized) > len(company_name) * 2:
            normalized = company_name

        return NormalizeResult(
            original=company_name,
            normalized=normalized,
            success=True
        )

    except Exception as e:
        return NormalizeResult(
            original=company_name,
            normalized=company_name,
            success=False,
            error=f"Anthropic API error: {str(e)}"
        )


def normalize_company_name(company_name: str) -> NormalizeResult:
    """
    Normalize a company name using Claude AI (via OpenRouter or Anthropic).

    Args:
        company_name: Raw company name to normalize

    Returns:
        NormalizeResult with normalized name
    """
    if not company_name or not company_name.strip():
        return NormalizeResult(
            original=company_name or "",
            normalized="",
            success=False,
            error="Empty company name"
        )

    company_name = company_name.strip()

    # Try OpenRouter first (preferred)
    if OPENROUTER_API_KEY:
        return normalize_via_openrouter(company_name)

    # Fall back to Anthropic direct
    if ANTHROPIC_API_KEY and ANTHROPIC_AVAILABLE:
        return normalize_via_anthropic(company_name)

    # No API available
    return NormalizeResult(
        original=company_name,
        normalized=company_name,
        success=False,
        error="No API key set. Add OPENROUTER_API_KEY or ANTHROPIC_API_KEY to .env"
    )


def normalize_batch(company_names: list[str], delay: float = 1.0, batch_size: int = None) -> list[NormalizeResult]:
    """
    Normalize a batch of company names efficiently using batched API calls.

    For 20,000 rows with batch_size=50, this makes ~400 API calls instead of 20,000.

    Args:
        company_names: List of company names to normalize
        delay: Delay between batch API calls in seconds
        batch_size: Number of names per API call (default from BATCH_SIZE env var)

    Returns:
        List of NormalizeResult objects
    """
    import time

    batch_size = batch_size or BATCH_SIZE

    # If OpenRouter is available, use batch processing
    if OPENROUTER_API_KEY:
        results = []
        total_batches = (len(company_names) + batch_size - 1) // batch_size

        for batch_num, i in enumerate(range(0, len(company_names), batch_size)):
            batch = company_names[i:i + batch_size]
            batch_results = normalize_batch_via_openrouter(batch)
            results.extend(batch_results)

            # Progress indicator
            print(f"  Batch {batch_num + 1}/{total_batches} complete ({len(results)}/{len(company_names)} names)")

            # Rate limit delay between batches
            if i + batch_size < len(company_names):
                time.sleep(delay)

        return results

    # Fallback to individual processing if no OpenRouter
    results = []
    for i, name in enumerate(company_names):
        result = normalize_company_name(name)
        results.append(result)

        if i < len(company_names) - 1:
            time.sleep(delay)

    return results


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python normalize_company_name.py <company_name>")
        print("       python normalize_company_name.py \"7 Gravity Inc\"")
        sys.exit(1)

    name = " ".join(sys.argv[1:])
    result = normalize_company_name(name)

    print(f"Original:   {result.original}")
    print(f"Normalized: {result.normalized}")
    print(f"Success:    {result.success}")
    if result.error:
        print(f"Error:      {result.error}")
