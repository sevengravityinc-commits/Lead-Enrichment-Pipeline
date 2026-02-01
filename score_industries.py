"""
Score Industries Script
Scores sub-industries for cold email lead generation viability using AI.
Uses OpenRouter (GPT-4o-mini) for cost-effective scoring.
"""

import os
import sys
import csv
import json
import re
import requests
from typing import Optional
from dataclasses import dataclass, asdict
from dotenv import load_dotenv
from collections import Counter

load_dotenv()

# API Configuration
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
DEFAULT_MODEL = os.getenv('SCORE_INDUSTRIES_MODEL', 'openai/gpt-4o-mini')


@dataclass
class IndustryScore:
    industry: str
    sub_industry: str
    lead_count: int
    ease_of_selling: int
    ease_of_fulfillment: int
    ltv_meets_threshold: str  # Yes/No
    tam_meets_threshold: str  # Yes/No
    total_score: int
    tier: str  # A/B/C
    reasoning: str


SCORING_PROMPT = """You are an expert at evaluating industries for cold email lead generation services.

Score each sub-industry below based on how viable they are as targets for selling B2B lead generation services via cold email.

SUB-INDUSTRIES TO SCORE:
{industries_list}

SCORING CRITERIA:

1. EASE OF SELLING (1-10): How receptive is this sub-industry to cold email outreach for lead gen services?
   - High (7-10): Businesses actively seek growth, decision makers accessible, accept cold outreach
   - Low (1-4): Heavily regulated, saturated with offers, rely on referrals only

2. EASE OF FULFILLMENT (1-10): How easy is it to generate quality leads for clients in this sub-industry?
   - High (7-10): Clear ICP, abundant prospect data, proven templates exist
   - Low (1-4): Niche/limited prospects, complex buying committees, highly localized

3. LTV POTENTIAL ($10K+ threshold): Does this sub-industry typically have client LTV of $10K+ for lead gen services?
   - Yes: High-ticket products, recurring revenue, long relationships
   - No: Low margins, one-time transactions, price-sensitive

4. TAM SUFFICIENT (50K+ businesses threshold): Is the total addressable market 50K+ businesses?
   - Yes: Large fragmented industry, many SMBs, national/global market
   - No: Small niche, few players, highly consolidated

OUTPUT FORMAT - Return a JSON array with this exact structure:
```json
[
  {{
    "sub_industry": "exact sub-industry name",
    "ease_of_selling": 7,
    "ease_of_fulfillment": 8,
    "ltv_meets_threshold": "Yes",
    "tam_meets_threshold": "Yes",
    "reasoning": "Brief 1-sentence explanation"
  }}
]
```

RULES:
- Score EVERY sub-industry listed - do not skip any
- Be realistic - most industries should score 4-7, few should be 9-10
- Consider cold email lead gen specifically, not general marketing
- "Other" or "N/A" categories should score low (3-4) with note "Insufficient specificity"

Score these sub-industries now:"""


def get_tier(ease_selling: int, ease_fulfillment: int, ltv: str, tam: str) -> str:
    """Calculate tier based on scores and thresholds."""
    total = ease_selling + ease_fulfillment
    ltv_met = ltv.lower() == "yes"
    tam_met = tam.lower() == "yes"

    if total >= 16 and ltv_met and tam_met:
        return "A"
    elif total >= 12 or ltv_met or tam_met:
        return "B"
    else:
        return "C"


def score_industries_batch(industries: list[tuple[str, str, int]], client=None) -> list[IndustryScore]:
    """Score a batch of industries using OpenRouter API (GPT-4o-mini)."""

    # Format industries for prompt
    industries_text = "\n".join([
        f"- {ind} > {sub_ind} ({count:,} leads)"
        for ind, sub_ind, count in industries
    ])

    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEFAULT_MODEL,
                "max_tokens": 4000,
                "messages": [{
                    "role": "user",
                    "content": SCORING_PROMPT.format(industries_list=industries_text)
                }]
            },
            timeout=60
        )

        response.raise_for_status()
        data = response.json()
        response_text = data['choices'][0]['message']['content']

        # Extract JSON from response
        json_match = re.search(r'\[[\s\S]*\]', response_text)
        if not json_match:
            print(f"Warning: Could not parse JSON from response")
            return []

        scores_data = json.loads(json_match.group())

        results = []
        for ind, sub_ind, count in industries:
            # Find matching score in response
            score_entry = None
            for entry in scores_data:
                if entry.get("sub_industry", "").lower() == sub_ind.lower():
                    score_entry = entry
                    break

            if score_entry:
                ease_selling = int(score_entry.get("ease_of_selling", 5))
                ease_fulfillment = int(score_entry.get("ease_of_fulfillment", 5))
                ltv = score_entry.get("ltv_meets_threshold", "No")
                tam = score_entry.get("tam_meets_threshold", "No")
                reasoning = score_entry.get("reasoning", "")

                results.append(IndustryScore(
                    industry=ind,
                    sub_industry=sub_ind,
                    lead_count=count,
                    ease_of_selling=ease_selling,
                    ease_of_fulfillment=ease_fulfillment,
                    ltv_meets_threshold=ltv,
                    tam_meets_threshold=tam,
                    total_score=ease_selling + ease_fulfillment,
                    tier=get_tier(ease_selling, ease_fulfillment, ltv, tam),
                    reasoning=reasoning
                ))
            else:
                # Default score if not found in response
                results.append(IndustryScore(
                    industry=ind,
                    sub_industry=sub_ind,
                    lead_count=count,
                    ease_of_selling=5,
                    ease_of_fulfillment=5,
                    ltv_meets_threshold="No",
                    tam_meets_threshold="No",
                    total_score=10,
                    tier="C",
                    reasoning="Could not parse score from API response"
                ))

        return results

    except Exception as e:
        print(f"API Error: {e}")
        # Return default scores for all industries in batch
        return [
            IndustryScore(
                industry=ind,
                sub_industry=sub_ind,
                lead_count=count,
                ease_of_selling=5,
                ease_of_fulfillment=5,
                ltv_meets_threshold="No",
                tam_meets_threshold="No",
                total_score=10,
                tier="C",
                reasoning=f"API error: {str(e)}"
            )
            for ind, sub_ind, count in industries
        ]


def extract_industries_from_csv(filepath: str) -> list[tuple[str, str, int]]:
    """Extract unique industry/sub-industry combinations with counts."""
    counter = Counter()

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            industry = row.get('Industry', '').strip()
            sub_industry = row.get('Sub Industry', '').strip()
            if industry and sub_industry:
                counter[(industry, sub_industry)] += 1

    # Convert to list of tuples
    return [(ind, sub_ind, count) for (ind, sub_ind), count in counter.items()]


def main(input_file: str):
    """Main function to score all industries."""

    if not OPENROUTER_API_KEY:
        print("Error: OPENROUTER_API_KEY not set in environment")
        sys.exit(1)

    print(f"Reading industries from: {input_file}")
    print(f"Using model: {DEFAULT_MODEL}")
    industries = extract_industries_from_csv(input_file)
    print(f"Found {len(industries)} unique sub-industries")

    # Sort by industry name for consistent batching
    industries.sort(key=lambda x: (x[0], x[1]))

    all_scores = []

    # Process in batches of 12
    batch_size = 12
    total_batches = (len(industries) + batch_size - 1) // batch_size

    for i in range(0, len(industries), batch_size):
        batch = industries[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"Scoring batch {batch_num}/{total_batches} ({len(batch)} industries)...")

        scores = score_industries_batch(batch)
        all_scores.extend(scores)

        # Save progress after each batch
        save_results(all_scores, input_file)

    # Final sort by tier and score
    all_scores.sort(key=lambda x: (
        {"A": 0, "B": 1, "C": 2}.get(x.tier, 3),
        -x.total_score,
        -x.lead_count
    ))

    save_results(all_scores, input_file)
    print_summary(all_scores)


def save_results(scores: list[IndustryScore], input_file: str):
    """Save results to CSV file."""
    # Determine output path
    input_dir = os.path.dirname(input_file)
    output_file = os.path.join(input_dir, "scored_industries.csv")

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "Industry", "Sub Industry", "Lead Count",
            "Ease of Selling", "Ease of Fulfillment",
            "LTV Meets Threshold", "TAM Meets Threshold",
            "Total Score", "Tier", "Reasoning"
        ])

        for score in scores:
            writer.writerow([
                score.industry,
                score.sub_industry,
                score.lead_count,
                score.ease_of_selling,
                score.ease_of_fulfillment,
                score.ltv_meets_threshold,
                score.tam_meets_threshold,
                score.total_score,
                score.tier,
                score.reasoning
            ])

    print(f"Results saved to: {output_file}")


def print_summary(scores: list[IndustryScore]):
    """Print summary of scoring results."""
    tier_counts = Counter(s.tier for s in scores)
    total_leads = sum(s.lead_count for s in scores)

    print("\n" + "="*60)
    print("SCORING SUMMARY")
    print("="*60)
    print(f"Total sub-industries scored: {len(scores)}")
    print(f"Total leads in file: {total_leads:,}")
    print()
    print("Tier Distribution:")
    for tier in ["A", "B", "C"]:
        count = tier_counts.get(tier, 0)
        tier_leads = sum(s.lead_count for s in scores if s.tier == tier)
        pct = count / len(scores) * 100 if scores else 0
        print(f"  Tier {tier}: {count} sub-industries ({pct:.1f}%) - {tier_leads:,} leads")

    print("\nTop 10 Tier A Sub-Industries:")
    tier_a = [s for s in scores if s.tier == "A"][:10]
    for i, s in enumerate(tier_a, 1):
        print(f"  {i}. {s.industry} > {s.sub_industry}")
        print(f"     Score: {s.total_score}/20 | Leads: {s.lead_count:,}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python score_industries.py <path-to-lead-csv>")
        print("Example: python score_industries.py .tmp/leads.csv")
        sys.exit(1)

    main(sys.argv[1])
