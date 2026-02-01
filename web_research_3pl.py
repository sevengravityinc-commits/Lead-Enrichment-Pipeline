"""
Web Research Script for 3PL Classification
Fetches company websites and analyzes content to categorize ambiguous leads.
"""

import csv
import re
import time
import requests
from pathlib import Path
import shutil
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

# Request settings
TIMEOUT = 10
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# Category indicators to look for on websites
WEBSITE_INDICATORS = {
    '3PL_FULFILLMENT': {
        'strong': [
            r'e-?commerce\s+fulfillment',
            r'order\s+fulfillment',
            r'pick\s*(and|&)\s*pack',
            r'shopify\s+integration',
            r'amazon\s+fba',
            r'subscription\s+box',
            r'dtc\s+(brand|fulfillment)',
            r'd2c\s+(brand|fulfillment)',
            r'kitting\s+(and|&)?\s*assembly',
            r'3pl\s+(services|provider|warehouse)',
        ],
        'medium': [
            r'fulfillment\s+center',
            r'online\s+retail',
            r'e-?commerce\s+logistics',
            r'same[- ]day\s+shipping',
            r'returns\s+management',
            r'inventory\s+management',
            r'wms|warehouse\s+management',
        ]
    },
    'PORT_TRANSIT': {
        'strong': [
            r'drayage',
            r'freight\s+forward',
            r'customs\s+(brokerage|clearance|broker)',
            r'ocean\s+freight',
            r'air\s+freight',
            r'intermodal',
            r'nvocc',
            r'ltl|ftl|truckload',
            r'port\s+(services|logistics|drayage)',
            r'import\s*/?\s*export',
        ],
        'medium': [
            r'freight\s+broker',
            r'container\s+shipping',
            r'international\s+shipping',
            r'cross[- ]border',
            r'carrier\s+network',
            r'fleet\s+management',
        ]
    },
    'SPECIALIZED_STORAGE': {
        'strong': [
            r'cold\s+storage',
            r'refrigerated\s+warehouse',
            r'temperature[- ]controlled',
            r'pharmaceutical\s+(warehousing|storage|logistics)',
            r'fda\s+(approved|compliant|certified)',
            r'hazmat|hazardous\s+materials',
            r'food[- ]grade\s+warehouse',
            r'cgmp',
            r'cold\s+chain',
            r'bonded\s+warehouse',
        ],
        'medium': [
            r'frozen\s+storage',
            r'climate[- ]controlled',
            r'perishable',
            r'usda\s+(approved|certified)',
            r'sqf\s+certified',
        ]
    }
}


@dataclass
class WebResearchResult:
    company_name: str
    website: str
    category: str
    confidence: str
    reasoning: str
    indicators_found: List[str]
    error: Optional[str] = None


def fetch_page(url: str) -> Optional[str]:
    """Fetch a webpage and return its text content."""
    try:
        # Ensure URL has protocol
        if not url.startswith('http'):
            url = 'https://' + url

        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return response.text
    except Exception as e:
        return None


def extract_text_content(html: str) -> str:
    """Extract relevant text from HTML."""
    soup = BeautifulSoup(html, 'html.parser')

    # Remove script and style elements
    for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
        tag.decompose()

    # Get text
    text = soup.get_text(separator=' ', strip=True)

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)

    return text.lower()


def find_services_page(html: str, base_url: str) -> Optional[str]:
    """Try to find and return URL to services page."""
    soup = BeautifulSoup(html, 'html.parser')

    service_patterns = [
        r'services?',
        r'what[- ]we[- ]do',
        r'solutions?',
        r'capabilities',
    ]

    for link in soup.find_all('a', href=True):
        link_text = link.get_text().lower()
        href = link['href'].lower()

        for pattern in service_patterns:
            if re.search(pattern, link_text) or re.search(pattern, href):
                return urljoin(base_url, link['href'])

    return None


def analyze_content(text: str) -> Tuple[str, str, List[str], Dict[str, int]]:
    """Analyze text content and return category, confidence, and indicators."""
    scores = {'3PL_FULFILLMENT': 0, 'PORT_TRANSIT': 0, 'SPECIALIZED_STORAGE': 0}
    found_indicators = []

    for category, patterns in WEBSITE_INDICATORS.items():
        for pattern in patterns['strong']:
            matches = re.findall(pattern, text)
            if matches:
                scores[category] += 3 * len(matches)
                found_indicators.append(f"{category}: {pattern}")

        for pattern in patterns['medium']:
            matches = re.findall(pattern, text)
            if matches:
                scores[category] += 2 * len(matches)

    # Determine category
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_category, top_score = sorted_scores[0]
    second_score = sorted_scores[1][1] if len(sorted_scores) > 1 else 0

    if top_score == 0:
        return 'NEEDS_REVIEW', 'Low', [], scores

    # Determine confidence
    if top_score >= 15 and top_score / max(second_score, 1) >= 2:
        confidence = 'High'
    elif top_score >= 6 and top_score > second_score:
        confidence = 'Medium'
    else:
        confidence = 'Low'

    return top_category, confidence, found_indicators[:5], scores


def research_company(company_name: str, website: str) -> WebResearchResult:
    """Research a single company by analyzing their website."""
    if not website:
        return WebResearchResult(
            company_name=company_name,
            website=website,
            category='NEEDS_REVIEW',
            confidence='Low',
            reasoning='No website URL provided',
            indicators_found=[],
            error='No URL'
        )

    # Fetch homepage
    html = fetch_page(website)
    if not html:
        return WebResearchResult(
            company_name=company_name,
            website=website,
            category='NEEDS_REVIEW',
            confidence='Low',
            reasoning='Could not fetch website',
            indicators_found=[],
            error='Fetch failed'
        )

    # Extract text from homepage
    all_text = extract_text_content(html)

    # Try to fetch services page for more info
    services_url = find_services_page(html, website)
    if services_url:
        services_html = fetch_page(services_url)
        if services_html:
            all_text += ' ' + extract_text_content(services_html)

    # Analyze content
    category, confidence, indicators, scores = analyze_content(all_text)

    # Build reasoning
    if category == 'NEEDS_REVIEW':
        reasoning = f'Website content inconclusive. Scores: {scores}'
    else:
        reasoning = f'Website analysis suggests {category}. Found: {", ".join(indicators[:3]) if indicators else "weak signals"}'

    return WebResearchResult(
        company_name=company_name,
        website=website,
        category=category,
        confidence=confidence,
        reasoning=reasoning,
        indicators_found=indicators
    )


def process_needs_review(input_csv: str, output_csv: str) -> Dict[str, List]:
    """Process the NEEDS_REVIEW CSV and re-classify based on web research."""
    results = {
        '3PL_FULFILLMENT': [],
        'PORT_TRANSIT': [],
        'SPECIALIZED_STORAGE': [],
        'NEEDS_REVIEW': []
    }

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Researching {len(rows)} companies...")

    for i, row in enumerate(rows):
        company_name = row.get('Company Name', '')
        website = row.get('Website', '')

        print(f"[{i+1}/{len(rows)}] Researching: {company_name}")

        result = research_company(company_name, website)

        # Update row with new classification
        row['Confidence'] = result.confidence
        row['Reasoning'] = result.reasoning
        row['Keywords Used'] = ', '.join(result.indicators_found)

        results[result.category].append(row)

        # Rate limiting
        time.sleep(0.5)

    # Save all results to separate files
    output_path = Path(output_csv).parent

    for category, items in results.items():
        if items:
            # For NEEDS_REVIEW, save to the specified output file
            if category == 'NEEDS_REVIEW':
                filepath = output_csv
            else:
                # Append reclassified items to existing category files
                filepath = output_path / f'{category.lower()}.csv'

            # Check if file exists to determine if we need headers
            file_exists = Path(filepath).exists() and category != 'NEEDS_REVIEW'

            mode = 'a' if file_exists else 'w'
            with open(filepath, mode, newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerows(items)

            if category != 'NEEDS_REVIEW':
                print(f"  Appended {len(items)} leads to {filepath}")

    return results


def process_low_confidence(input_csv: str, category: str) -> List[dict]:
    """Process low confidence items from a category CSV."""
    reclassified = []

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    low_confidence = [r for r in rows if r.get('Confidence') == 'Low']
    print(f"Found {len(low_confidence)} low-confidence {category} leads to verify...")

    for i, row in enumerate(low_confidence):
        company_name = row.get('Company Name', '')
        website = row.get('Website', '')

        print(f"[{i+1}/{len(low_confidence)}] Verifying: {company_name}")

        result = research_company(company_name, website)

        if result.category != category:
            # Reclassified!
            row['Confidence'] = result.confidence
            row['Reasoning'] = f"RECLASSIFIED from {category}: {result.reasoning}"
            row['Keywords Used'] = ', '.join(result.indicators_found)
            row['_new_category'] = result.category
            reclassified.append(row)

        time.sleep(0.5)

    return reclassified


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage: python web_research_3pl.py <needs_review.csv> [output.csv]")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2] if len(sys.argv) > 2 else input_csv.replace('.csv', '_updated.csv')

    results = process_needs_review(input_csv, output_csv)

    print("\n" + "="*50)
    print("WEB RESEARCH RESULTS")
    print("="*50)
    for cat, items in results.items():
        print(f"{cat}: {len(items)}")
    print("="*50)
