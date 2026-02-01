"""
ICP (Ideal Customer Profile) Scorer

Scores companies against configurable ICP criteria:
- Employee count range
- Industry match
- Geographic location
- Revenue range

Returns a 0-100 score and tier assignment (A/B/C).

Usage:
    python icp_scorer.py .tmp/leads/filtered.json --config configs/example_marketing.json
"""

import os
import sys
import json
import argparse
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ICPScore:
    """ICP scoring result"""
    total_score: int
    tier: str  # "A", "B", "C", "D"
    breakdown: Dict[str, float]
    meets_threshold: bool
    reasoning: List[str]


def parse_revenue(revenue_str: str) -> Optional[float]:
    """Parse revenue string to numeric value in millions"""
    if not revenue_str:
        return None

    revenue_str = str(revenue_str).upper().strip()

    # Remove currency symbols and commas
    revenue_str = revenue_str.replace("$", "").replace(",", "").replace(" ", "")

    multiplier = 1
    if "B" in revenue_str:
        multiplier = 1000
        revenue_str = revenue_str.replace("B", "")
    elif "M" in revenue_str:
        multiplier = 1
        revenue_str = revenue_str.replace("M", "")
    elif "K" in revenue_str:
        multiplier = 0.001
        revenue_str = revenue_str.replace("K", "")

    try:
        return float(revenue_str) * multiplier
    except:
        return None


def score_employee_range(
    employee_count: Optional[int],
    min_employees: int,
    max_employees: int
) -> Tuple[float, str]:
    """
    Score based on employee count.

    Returns:
        Tuple of (score 0-100, reasoning)
    """
    if employee_count is None:
        return 50, "Employee count unknown, assuming mid-range"

    if min_employees <= employee_count <= max_employees:
        # Perfect fit
        return 100, f"Employee count {employee_count} within ideal range ({min_employees}-{max_employees})"

    # Below minimum
    if employee_count < min_employees:
        ratio = employee_count / min_employees
        score = max(0, ratio * 60)  # Max 60 if below minimum
        return score, f"Employee count {employee_count} below minimum ({min_employees})"

    # Above maximum
    if employee_count > max_employees:
        ratio = max_employees / employee_count
        score = max(0, ratio * 60)  # Max 60 if above maximum
        return score, f"Employee count {employee_count} above maximum ({max_employees})"

    return 50, "Employee count scoring error"


def score_industry_match(
    industry: Optional[str],
    included: List[str],
    excluded: List[str]
) -> Tuple[float, str]:
    """
    Score based on industry match.

    Returns:
        Tuple of (score 0-100, reasoning)
    """
    if not industry:
        return 50, "Industry unknown"

    industry_lower = industry.lower()

    # Check exclusions first
    for exc in excluded:
        if exc.lower() in industry_lower:
            return 0, f"Industry '{industry}' matches exclusion: {exc}"

    # Check inclusions
    for inc in included:
        if inc.lower() in industry_lower:
            return 100, f"Industry '{industry}' matches target: {inc}"

    # Partial match check
    for inc in included:
        # Check if any word matches
        inc_words = inc.lower().split()
        ind_words = industry_lower.split()
        if any(iw in ind_words for iw in inc_words):
            return 70, f"Industry '{industry}' partially matches target industries"

    return 30, f"Industry '{industry}' not in target list"


def score_geo_match(
    country: Optional[str],
    target_countries: List[str],
    exclude_countries: List[str] = None
) -> Tuple[float, str]:
    """
    Score based on geographic location.

    Returns:
        Tuple of (score 0-100, reasoning)
    """
    if not country:
        return 50, "Country unknown"

    country_upper = country.upper().strip()

    # Common country code mappings
    country_mappings = {
        "UNITED STATES": "US",
        "UNITED KINGDOM": "UK",
        "CANADA": "CA",
        "AUSTRALIA": "AU",
        "GERMANY": "DE",
        "FRANCE": "FR",
        "NETHERLANDS": "NL",
        "USA": "US",
        "U.S.": "US",
        "U.S.A.": "US",
        "UK": "UK",
        "GB": "UK",
    }

    # Normalize country
    normalized = country_mappings.get(country_upper, country_upper)

    # Check exclusions
    if exclude_countries:
        for exc in exclude_countries:
            if exc.upper() == normalized or exc.upper() in country_upper:
                return 0, f"Country '{country}' is excluded"

    # Check target countries
    target_upper = [t.upper() for t in target_countries]

    if normalized in target_upper:
        return 100, f"Country '{country}' is target geography"

    # Check if full country name matches
    for target in target_countries:
        if target.upper() in country_upper:
            return 100, f"Country '{country}' matches target: {target}"

    return 20, f"Country '{country}' not in target geographies"


def score_revenue_range(
    revenue: Optional[str],
    min_revenue: str,
    max_revenue: str
) -> Tuple[float, str]:
    """
    Score based on revenue range.

    Returns:
        Tuple of (score 0-100, reasoning)
    """
    revenue_val = parse_revenue(revenue)
    min_val = parse_revenue(min_revenue)
    max_val = parse_revenue(max_revenue)

    if revenue_val is None:
        return 50, "Revenue unknown"

    if min_val is None or max_val is None:
        return 50, "Revenue criteria not properly configured"

    if min_val <= revenue_val <= max_val:
        return 100, f"Revenue ${revenue_val}M within ideal range (${min_val}M-${max_val}M)"

    if revenue_val < min_val:
        ratio = revenue_val / min_val
        score = max(0, ratio * 60)
        return score, f"Revenue ${revenue_val}M below minimum (${min_val}M)"

    if revenue_val > max_val:
        ratio = max_val / revenue_val
        score = max(0, ratio * 60)
        return score, f"Revenue ${revenue_val}M above maximum (${max_val}M)"

    return 50, "Revenue scoring error"


def calculate_icp_score(
    lead: Dict[str, Any],
    criteria: Dict[str, Any],
    min_threshold: int = 60
) -> ICPScore:
    """
    Calculate ICP score for a lead.

    Args:
        lead: Lead data dict
        criteria: ICP criteria from config
        min_threshold: Minimum score to pass (default: 60)

    Returns:
        ICPScore with breakdown and tier
    """
    breakdown = {}
    reasoning = []
    total_weight = 0
    weighted_score = 0

    # Employee range scoring
    emp_criteria = criteria.get("employee_range", {})
    if emp_criteria:
        weight = emp_criteria.get("weight", 25)
        score, reason = score_employee_range(
            lead.get("employee_count"),
            emp_criteria.get("min", 1),
            emp_criteria.get("max", 10000)
        )
        breakdown["employee_range"] = score
        weighted_score += score * weight / 100
        total_weight += weight
        reasoning.append(f"Employees: {reason}")

    # Industry scoring
    ind_criteria = criteria.get("industries", {})
    if ind_criteria:
        weight = ind_criteria.get("weight", 30)
        score, reason = score_industry_match(
            lead.get("industry"),
            ind_criteria.get("included", []),
            ind_criteria.get("excluded", [])
        )
        breakdown["industry"] = score
        weighted_score += score * weight / 100
        total_weight += weight
        reasoning.append(f"Industry: {reason}")

    # Geo scoring
    geo_criteria = criteria.get("geo", {})
    if geo_criteria:
        weight = geo_criteria.get("weight", 15)
        score, reason = score_geo_match(
            lead.get("location_country"),
            geo_criteria.get("countries", []),
            geo_criteria.get("exclude_countries", [])
        )
        breakdown["geo"] = score
        weighted_score += score * weight / 100
        total_weight += weight
        reasoning.append(f"Geo: {reason}")

    # Revenue scoring
    rev_criteria = criteria.get("revenue_range", {})
    if rev_criteria and rev_criteria.get("min") and rev_criteria.get("max"):
        weight = rev_criteria.get("weight", 20)
        score, reason = score_revenue_range(
            lead.get("revenue_range"),
            rev_criteria.get("min"),
            rev_criteria.get("max")
        )
        breakdown["revenue"] = score
        weighted_score += score * weight / 100
        total_weight += weight
        reasoning.append(f"Revenue: {reason}")

    # Normalize if weights don't sum to 100
    if total_weight > 0 and total_weight != 100:
        weighted_score = weighted_score * 100 / total_weight

    total_score = int(round(weighted_score))

    # Assign tier
    if total_score >= 80:
        tier = "A"
    elif total_score >= 60:
        tier = "B"
    elif total_score >= 40:
        tier = "C"
    else:
        tier = "D"

    return ICPScore(
        total_score=total_score,
        tier=tier,
        breakdown=breakdown,
        meets_threshold=total_score >= min_threshold,
        reasoning=reasoning
    )


def score_leads(
    input_path: str,
    output_path: str,
    config: Dict[str, Any]
) -> Dict:
    """
    Score all leads against ICP criteria.

    Args:
        input_path: Path to filtered leads JSON
        output_path: Path for scored output
        config: Campaign config

    Returns:
        Summary statistics
    """
    # Load input
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    leads = data.get("leads", [])
    total = len(leads)

    print(f"Scoring {total} leads against ICP...")

    # Get scorer config
    scorer_config = config.get("classifiers", {}).get("icp_scorer", {})

    if not scorer_config.get("enabled", True):
        print("ICP scorer disabled in config, skipping...")
        output_data = {
            "icp_summary": {"skipped": True, "reason": "disabled in config"},
            "leads": leads
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, default=str)
        return {"skipped": True}

    criteria = scorer_config.get("criteria", {})
    min_threshold = scorer_config.get("min_score_threshold", 60)

    # Score leads
    tier_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
    scored_leads = []

    for i, lead in enumerate(leads):
        score_result = calculate_icp_score(lead, criteria, min_threshold)

        lead["_icp_score"] = score_result.total_score
        lead["_icp_tier"] = score_result.tier
        lead["_icp_breakdown"] = score_result.breakdown
        lead["_icp_meets_threshold"] = score_result.meets_threshold
        lead["_icp_reasoning"] = score_result.reasoning

        tier_counts[score_result.tier] += 1
        scored_leads.append(lead)

        # Progress
        if (i + 1) % 100 == 0:
            print(f"Scored {i + 1}/{total} leads...")

    # Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    summary = {
        "total": total,
        "tier_a": tier_counts["A"],
        "tier_b": tier_counts["B"],
        "tier_c": tier_counts["C"],
        "tier_d": tier_counts["D"],
        "above_threshold": sum(1 for l in scored_leads if l.get("_icp_meets_threshold")),
        "min_threshold": min_threshold
    }

    output_data = {
        "icp_summary": summary,
        "leads": scored_leads
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nICP Scoring Results:")
    print(f"  Tier A (80+): {tier_counts['A']}")
    print(f"  Tier B (60-79): {tier_counts['B']}")
    print(f"  Tier C (40-59): {tier_counts['C']}")
    print(f"  Tier D (<40): {tier_counts['D']}")
    print(f"  Above threshold ({min_threshold}+): {summary['above_threshold']}")
    print(f"\nSaved to: {output_path}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Score leads against ICP criteria")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--config", "-c", required=True, help="Campaign config file")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/scored.json")

    args = parser.parse_args()

    # Load config
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    summary = score_leads(args.input, args.output, config)

    print("\nSummary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
