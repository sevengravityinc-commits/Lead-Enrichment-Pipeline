"""
Unified Lead Quality Score Calculator

Combines multiple signals into a single 0-100 score:
- ICP Fit Score (35%)
- Decision Maker Quality (25%)
- Email Deliverability (20%)
- Intent Signals (10%) - if enabled
- Data Completeness (10%)

Usage:
    python calculate_lead_score.py .tmp/leads/enriched.json --config configs/example_marketing.json
"""

import os
import sys
import json
import argparse
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class LeadScore:
    """Unified lead quality score"""
    total_score: int
    tier: str  # "A", "B", "C", "D"
    breakdown: Dict[str, float]
    campaign_ready: bool


# Seniority level scores
SENIORITY_SCORES = {
    "c_suite": 100,
    "ceo": 100,
    "founder": 100,
    "co-founder": 100,
    "president": 100,
    "owner": 100,
    "vp": 90,
    "vice president": 90,
    "cmo": 85,
    "cro": 85,
    "director": 70,
    "head": 70,
    "manager": 50,
    "lead": 50,
    "senior": 40,
    "associate": 30,
    "coordinator": 20,
    "assistant": 15,
    "intern": 10,
    "unknown": 30
}


def detect_seniority(title: str) -> str:
    """Detect seniority level from job title"""
    if not title:
        return "unknown"

    title_lower = title.lower()

    # Check in order of priority
    if any(x in title_lower for x in ["ceo", "chief executive"]):
        return "ceo"
    if any(x in title_lower for x in ["founder", "co-founder", "cofounder"]):
        return "founder"
    if "president" in title_lower and "vice" not in title_lower:
        return "president"
    if "owner" in title_lower:
        return "owner"
    if any(x in title_lower for x in ["cmo", "chief marketing"]):
        return "cmo"
    if any(x in title_lower for x in ["cro", "chief revenue"]):
        return "cro"
    if any(x in title_lower for x in ["vp ", "v.p.", "vice president"]):
        return "vp"
    if any(x in title_lower for x in ["director", "head of"]):
        return "director"
    if "manager" in title_lower:
        return "manager"
    if "lead" in title_lower:
        return "lead"
    if "senior" in title_lower:
        return "senior"
    if "associate" in title_lower:
        return "associate"
    if "coordinator" in title_lower:
        return "coordinator"
    if "assistant" in title_lower:
        return "assistant"
    if "intern" in title_lower:
        return "intern"

    return "unknown"


def score_decision_maker_quality(lead: Dict) -> tuple:
    """
    Score based on decision-maker title/seniority.

    Returns:
        Tuple of (score 0-100, seniority_level)
    """
    title = lead.get("title") or lead.get("headline", "")
    seniority = detect_seniority(title)
    score = SENIORITY_SCORES.get(seniority, 30)

    # Adjust by company size
    employee_count = lead.get("employee_count")
    if employee_count:
        if employee_count <= 20:
            # Small company: boost Manager/Director scores
            if seniority in ["manager", "director"]:
                score = min(100, score + 20)
        elif employee_count > 200:
            # Large company: reduce Manager scores
            if seniority == "manager":
                score = max(0, score - 20)

    return score, seniority


def score_email_deliverability(lead: Dict) -> int:
    """
    Score based on email status.

    Returns:
        Score 0-100
    """
    email = lead.get("email")
    email_verified = lead.get("_email_verified")
    email_found = lead.get("_email_found")

    if not email:
        return 0

    # Check verification status
    if email_verified is True or email_found is True:
        return 100
    elif email_verified is False:
        return 40

    # Check email domain
    email_lower = email.lower()
    if any(d in email_lower for d in ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]):
        return 50  # Personal email - lower score

    # Has email but unverified
    return 70


def score_data_completeness(lead: Dict) -> int:
    """
    Score based on data completeness.

    Returns:
        Score 0-100
    """
    score = 0
    max_score = 100

    # Essential fields (60 points)
    if lead.get("email"):
        score += 20
    if lead.get("company_name"):
        score += 15
    if lead.get("first_name") or lead.get("full_name"):
        score += 15
    if lead.get("title"):
        score += 10

    # Useful fields (30 points)
    if lead.get("linkedin_url"):
        score += 10
    if lead.get("phone"):
        score += 5
    if lead.get("company_domain"):
        score += 5
    if lead.get("company_linkedin_url"):
        score += 5
    if lead.get("industry"):
        score += 5

    # Bonus fields (10 points)
    if lead.get("employee_count"):
        score += 5
    if lead.get("location_country"):
        score += 5

    return min(100, score)


def calculate_unified_score(
    lead: Dict,
    config: Dict,
    weights: Dict = None
) -> LeadScore:
    """
    Calculate unified lead quality score.

    Default weights:
    - ICP Fit: 35%
    - Decision Maker: 25%
    - Email: 20%
    - Intent: 10%
    - Completeness: 10%
    """
    weights = weights or {
        "icp": 35,
        "decision_maker": 25,
        "email": 20,
        "intent": 10,
        "completeness": 10
    }

    breakdown = {}
    weighted_score = 0
    total_weight = 0

    # ICP Score (from previous step)
    icp_score = lead.get("_icp_score", 50)
    breakdown["icp"] = icp_score
    weighted_score += icp_score * weights["icp"] / 100
    total_weight += weights["icp"]

    # Decision Maker Quality
    dm_score, seniority = score_decision_maker_quality(lead)
    breakdown["decision_maker"] = dm_score
    lead["_seniority_level"] = seniority
    weighted_score += dm_score * weights["decision_maker"] / 100
    total_weight += weights["decision_maker"]

    # Email Deliverability
    email_score = score_email_deliverability(lead)
    breakdown["email"] = email_score
    weighted_score += email_score * weights["email"] / 100
    total_weight += weights["email"]

    # Intent Signals (if available)
    intent_config = config.get("classifiers", {}).get("intent_signals", {})
    if intent_config.get("enabled"):
        intent_score = lead.get("_intent_score", 50)
        breakdown["intent"] = intent_score
        weighted_score += intent_score * weights["intent"] / 100
        total_weight += weights["intent"]
    else:
        # Redistribute intent weight to other factors
        total_weight -= weights["intent"]

    # Data Completeness
    completeness_score = score_data_completeness(lead)
    breakdown["completeness"] = completeness_score
    weighted_score += completeness_score * weights["completeness"] / 100
    total_weight += weights["completeness"]

    # Normalize
    if total_weight > 0:
        total_score = int(round(weighted_score * 100 / total_weight))
    else:
        total_score = 0

    # Clamp to 0-100
    total_score = max(0, min(100, total_score))

    # Assign tier
    if total_score >= 80:
        tier = "A"
    elif total_score >= 60:
        tier = "B"
    elif total_score >= 40:
        tier = "C"
    else:
        tier = "D"

    # Determine campaign readiness
    min_quality = config.get("output", {}).get("min_quality_score", 50)
    campaign_ready = (
        total_score >= min_quality and
        lead.get("email") is not None and
        lead.get("_icp_tier") in ["A", "B", "C"]
    )

    return LeadScore(
        total_score=total_score,
        tier=tier,
        breakdown=breakdown,
        campaign_ready=campaign_ready
    )


def score_all_leads(
    input_path: str,
    output_path: str,
    config: Dict[str, Any]
) -> Dict:
    """
    Calculate unified scores for all leads.

    Args:
        input_path: Path to enriched leads JSON
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

    print(f"Calculating unified scores for {total} leads...")

    # Score leads
    tier_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
    scored_leads = []
    campaign_ready_count = 0

    for i, lead in enumerate(leads):
        score_result = calculate_unified_score(lead, config)

        lead["_quality_score"] = score_result.total_score
        lead["_quality_tier"] = score_result.tier
        lead["_quality_breakdown"] = score_result.breakdown
        lead["_campaign_ready"] = score_result.campaign_ready

        tier_counts[score_result.tier] += 1
        if score_result.campaign_ready:
            campaign_ready_count += 1

        scored_leads.append(lead)

        if (i + 1) % 100 == 0:
            print(f"Scored {i + 1}/{total} leads...")

    # Sort by quality score (highest first)
    scored_leads.sort(key=lambda x: x.get("_quality_score", 0), reverse=True)

    # Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    summary = {
        "total": total,
        "tier_a": tier_counts["A"],
        "tier_b": tier_counts["B"],
        "tier_c": tier_counts["C"],
        "tier_d": tier_counts["D"],
        "campaign_ready": campaign_ready_count,
        "avg_score": round(sum(l.get("_quality_score", 0) for l in scored_leads) / total, 1) if total > 0 else 0
    }

    output_data = {
        "scoring_summary": summary,
        "leads": scored_leads
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nUnified Scoring Results:")
    print(f"  Tier A (80+): {tier_counts['A']}")
    print(f"  Tier B (60-79): {tier_counts['B']}")
    print(f"  Tier C (40-59): {tier_counts['C']}")
    print(f"  Tier D (<40): {tier_counts['D']}")
    print(f"  Campaign Ready: {campaign_ready_count}")
    print(f"  Average Score: {summary['avg_score']}")
    print(f"\nSaved to: {output_path}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Calculate unified lead quality scores")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--config", "-c", required=True, help="Campaign config file")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/final_scored.json")

    args = parser.parse_args()

    # Load config
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    summary = score_all_leads(args.input, args.output, config)

    print("\nSummary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
