"""
Company Type Filter

Config-driven filter to remove false positives from lead lists.
You provide the filter criteria (keywords, domains, industries) in the config.

Usage:
    python company_type_filter.py .tmp/leads/deduped.json --config configs/example_marketing.json
"""

import os
import sys
import json
import argparse
import re
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class FilterResult:
    """Result of company type classification"""
    company_name: str
    filter_result: str  # "keep", "exclude"
    match_type: str  # "keyword", "domain", "industry", "include_match", "default"
    confidence: str  # "high", "medium", "low"
    reason: str


def normalize_text(text: str) -> str:
    """Normalize text for matching"""
    if not text:
        return ""
    # Lowercase, remove special chars except spaces
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def keyword_match(text: str, keywords: List[str]) -> Optional[str]:
    """
    Check if text contains any of the keywords.
    Returns the matched keyword or None.
    """
    if not text:
        return None

    text_normalized = normalize_text(text)

    for keyword in keywords:
        keyword_normalized = normalize_text(keyword)
        if keyword_normalized in text_normalized:
            return keyword
        # Also check word boundary match
        if re.search(r'\b' + re.escape(keyword_normalized) + r'\b', text_normalized):
            return keyword

    return None


def filter_company(
    lead: Dict,
    include_keywords: List[str],
    exclude_keywords: List[str],
    include_industries: List[str],
    exclude_industries: List[str],
    competitor_domains: List[str]
) -> FilterResult:
    """
    Filter a single company based on config criteria.

    Logic (in order):
    1. Competitor domain → exclude (high confidence)
    2. Exclude keyword match → exclude (high confidence)
    3. Exclude industry match → exclude (medium confidence)
    4. Include keyword match → keep (high confidence)
    5. Include industry match → keep (medium confidence)
    6. Default → keep (low confidence)
    """
    company_name = lead.get("company_name", "")
    domain = lead.get("company_domain", "")
    industry = lead.get("industry", "")

    # 1. Check competitor domains
    if domain:
        domain_lower = domain.lower()
        for comp_domain in competitor_domains:
            if comp_domain.lower() == domain_lower:
                return FilterResult(
                    company_name=company_name,
                    filter_result="exclude",
                    match_type="domain",
                    confidence="high",
                    reason=f"Competitor domain: {domain}"
                )

    # 2. Check exclude keywords in company name
    if exclude_keywords:
        match = keyword_match(company_name, exclude_keywords)
        if match:
            return FilterResult(
                company_name=company_name,
                filter_result="exclude",
                match_type="keyword",
                confidence="high",
                reason=f"Exclude keyword match: '{match}'"
            )

    # 3. Check exclude industries
    if exclude_industries and industry:
        industry_lower = industry.lower()
        for excl_ind in exclude_industries:
            if excl_ind.lower() in industry_lower or industry_lower in excl_ind.lower():
                return FilterResult(
                    company_name=company_name,
                    filter_result="exclude",
                    match_type="industry",
                    confidence="medium",
                    reason=f"Exclude industry match: '{industry}'"
                )

    # 4. Check include keywords (positive match)
    if include_keywords:
        match = keyword_match(company_name, include_keywords)
        if match:
            return FilterResult(
                company_name=company_name,
                filter_result="keep",
                match_type="include_match",
                confidence="high",
                reason=f"Include keyword match: '{match}'"
            )

    # 5. Check include industries
    if include_industries and industry:
        industry_lower = industry.lower()
        for incl_ind in include_industries:
            if incl_ind.lower() in industry_lower or industry_lower in incl_ind.lower():
                return FilterResult(
                    company_name=company_name,
                    filter_result="keep",
                    match_type="industry",
                    confidence="medium",
                    reason=f"Include industry match: '{industry}'"
                )

    # 6. Default: keep (nothing matched)
    return FilterResult(
        company_name=company_name,
        filter_result="keep",
        match_type="default",
        confidence="low",
        reason="No filter criteria matched"
    )


def filter_companies(
    input_path: str,
    output_path: str,
    config: Dict[str, Any]
) -> Dict:
    """
    Filter companies based on config criteria.

    Config structure:
    {
        "classifiers": {
            "company_filter": {
                "enabled": true,
                "include_keywords": ["marketing", "agency", "digital"],
                "exclude_keywords": ["staffing", "recruiting", "temp"],
                "include_industries": ["Marketing", "Advertising"],
                "exclude_industries": ["Staffing", "Recruiting"],
                "competitor_domains": ["competitor1.com"]
            }
        }
    }
    """
    # Load input
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    leads = data.get("leads", [])
    total = len(leads)

    print(f"Filtering {total} leads...")

    # Get filter config
    filter_config = config.get("classifiers", {}).get("company_filter", {})

    if not filter_config.get("enabled", True):
        print("Company filter disabled in config, passing all leads through...")
        output_data = {
            "filter_summary": {"skipped": True, "reason": "disabled in config"},
            "leads": leads
        }
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, default=str)
        return {"skipped": True}

    # Extract filter criteria from config
    include_keywords = filter_config.get("include_keywords", [])
    exclude_keywords = filter_config.get("exclude_keywords", [
        # Default exclusions if not specified
        "staffing", "recruiting", "recruitment", "headhunter",
        "temp agency", "talent acquisition", "hr solutions"
    ])
    include_industries = filter_config.get("include_industries", [])
    exclude_industries = filter_config.get("exclude_industries", [])
    competitor_domains = filter_config.get("competitor_domains", [])

    # Also support legacy "exclude_types" mapping to keywords
    exclude_types = filter_config.get("exclude_types", [])
    type_to_keywords = {
        "staffing_agency": ["staffing", "temp agency", "temporary staffing"],
        "recruiting_firm": ["recruiting", "recruitment", "headhunter", "talent acquisition"],
        "consulting_firm": ["consulting", "consultants", "advisory"],
        "competitor": []  # handled via competitor_domains
    }
    for exc_type in exclude_types:
        exclude_keywords.extend(type_to_keywords.get(exc_type, []))

    # Deduplicate keywords
    exclude_keywords = list(set(exclude_keywords))
    include_keywords = list(set(include_keywords))

    print(f"Filter criteria:")
    print(f"  Include keywords: {include_keywords[:5]}{'...' if len(include_keywords) > 5 else ''}")
    print(f"  Exclude keywords: {exclude_keywords[:5]}{'...' if len(exclude_keywords) > 5 else ''}")
    print(f"  Include industries: {include_industries}")
    print(f"  Exclude industries: {exclude_industries}")
    print(f"  Competitor domains: {len(competitor_domains)}")

    # Process leads
    kept = []
    excluded = []

    for i, lead in enumerate(leads):
        result = filter_company(
            lead,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            include_industries=include_industries,
            exclude_industries=exclude_industries,
            competitor_domains=competitor_domains
        )

        lead["_filter_result"] = result.filter_result
        lead["_filter_reason"] = result.reason
        lead["_filter_confidence"] = result.confidence
        lead["_filter_match_type"] = result.match_type

        if result.filter_result == "exclude":
            excluded.append(lead)
        else:
            kept.append(lead)

        # Progress
        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{total} leads...")

    # Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    summary = {
        "total_input": total,
        "kept": len(kept),
        "excluded": len(excluded),
        "output_count": len(kept),
        "filter_criteria": {
            "include_keywords": include_keywords,
            "exclude_keywords": exclude_keywords,
            "include_industries": include_industries,
            "exclude_industries": exclude_industries,
            "competitor_domains": len(competitor_domains)
        }
    }

    output_data = {
        "filter_summary": summary,
        "excluded_leads": excluded,  # Keep for review
        "leads": kept
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nFilter Results:")
    print(f"  Kept: {len(kept)}")
    print(f"  Excluded: {len(excluded)}")
    print(f"\nSaved {len(kept)} leads to: {output_path}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Filter companies by type (config-driven)")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--config", "-c", required=True, help="Campaign config file")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/filtered.json")

    args = parser.parse_args()

    # Load config
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    summary = filter_companies(args.input, args.output, config)

    print("\nSummary:")
    for key, value in summary.items():
        if key != "filter_criteria":
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
