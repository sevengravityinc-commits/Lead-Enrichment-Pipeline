"""
Company Deduplication Module

Identifies and merges duplicate companies using:
1. Exact domain matching
2. Fuzzy company name matching (85% threshold)
3. Normalized company name comparison

Usage:
    python deduplicate_companies.py .tmp/leads/normalized.json --output .tmp/leads/deduped.json
"""

import os
import sys
import json
import re
import argparse
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from difflib import SequenceMatcher


# Common company suffixes to normalize
COMPANY_SUFFIXES = [
    # English
    "inc", "inc.", "incorporated",
    "llc", "l.l.c.", "l.l.c",
    "ltd", "ltd.", "limited",
    "corp", "corp.", "corporation",
    "co", "co.", "company",
    "plc", "p.l.c.",
    "llp", "l.l.p.",
    "lp", "l.p.",
    "pllc",
    "pc", "p.c.",
    # European
    "gmbh", "ag", "sa", "sarl", "srl", "bv", "nv",
    # Other
    "pty", "pty ltd", "pvt", "pvt ltd", "private limited",
]

# Words to remove for comparison
NOISE_WORDS = [
    "the", "a", "an", "and", "&",
    "group", "holding", "holdings",
    "international", "global", "worldwide",
    "solutions", "services", "consulting",
    "technologies", "technology", "tech",
    "digital", "media", "agency",
]


def normalize_company_name(name: str) -> str:
    """
    Normalize company name for comparison.
    Removes suffixes, noise words, and standardizes format.
    """
    if not name:
        return ""

    # Lowercase
    normalized = name.lower().strip()

    # Remove special characters except spaces
    normalized = re.sub(r'[^\w\s]', ' ', normalized)

    # Remove company suffixes
    for suffix in COMPANY_SUFFIXES:
        # Remove suffix at end of string
        pattern = r'\s+' + re.escape(suffix) + r'$'
        normalized = re.sub(pattern, '', normalized)

    # Remove noise words
    words = normalized.split()
    words = [w for w in words if w not in NOISE_WORDS]
    normalized = ' '.join(words)

    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    return normalized


def extract_domain(lead: Dict) -> Optional[str]:
    """Extract domain from lead data"""
    # Try company_domain first
    if lead.get("company_domain"):
        return lead["company_domain"].lower().strip()

    # Try email domain
    if lead.get("email") and "@" in lead["email"]:
        return lead["email"].split("@")[1].lower().strip()

    return None


def fuzzy_match(s1: str, s2: str) -> float:
    """Calculate similarity ratio between two strings"""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1, s2).ratio()


def find_duplicates(
    leads: List[Dict],
    threshold: float = 0.85
) -> Tuple[Dict[str, List[int]], Dict[int, str]]:
    """
    Find duplicate companies in lead list.

    Returns:
        - groups: Dict mapping group_id to list of lead indices
        - lead_to_group: Dict mapping lead index to group_id
    """
    n = len(leads)
    groups = {}
    lead_to_group = {}
    group_counter = 0

    # First pass: group by exact domain match
    domain_groups = defaultdict(list)
    for i, lead in enumerate(leads):
        domain = extract_domain(lead)
        if domain:
            # Skip generic email domains
            if domain not in ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]:
                domain_groups[domain].append(i)

    # Create groups from domain matches
    for domain, indices in domain_groups.items():
        if len(indices) > 1:
            group_id = f"domain_{domain}"
            groups[group_id] = indices
            for idx in indices:
                lead_to_group[idx] = group_id

    # Second pass: fuzzy match company names for ungrouped leads
    ungrouped = [i for i in range(n) if i not in lead_to_group]

    # Pre-compute normalized names
    normalized_names = {}
    for i in ungrouped:
        name = leads[i].get("company_name", "")
        normalized_names[i] = normalize_company_name(name)

    # Compare ungrouped leads
    processed = set()
    for i in ungrouped:
        if i in processed:
            continue

        name_i = normalized_names[i]
        if not name_i:
            continue

        # Find matches
        matches = [i]
        for j in ungrouped:
            if j <= i or j in processed:
                continue

            name_j = normalized_names[j]
            if not name_j:
                continue

            # Check similarity
            similarity = fuzzy_match(name_i, name_j)
            if similarity >= threshold:
                matches.append(j)
                processed.add(j)

        # Create group if matches found
        if len(matches) > 1:
            group_id = f"fuzzy_{group_counter}"
            group_counter += 1
            groups[group_id] = matches
            for idx in matches:
                lead_to_group[idx] = group_id
                processed.add(idx)

    return groups, lead_to_group


def merge_duplicates(
    leads: List[Dict],
    groups: Dict[str, List[int]],
    lead_to_group: Dict[int, str]
) -> List[Dict]:
    """
    Merge duplicate leads into single records.
    Keeps the lead with most complete data as primary.
    """
    merged_leads = []
    processed_groups = set()

    for i, lead in enumerate(leads):
        # Check if part of a group
        group_id = lead_to_group.get(i)

        if group_id:
            if group_id in processed_groups:
                continue

            # Get all leads in group
            group_indices = groups[group_id]
            group_leads = [leads[idx] for idx in group_indices]

            # Score leads by completeness
            def completeness_score(l):
                score = 0
                if l.get("email"): score += 2
                if l.get("first_name"): score += 1
                if l.get("last_name"): score += 1
                if l.get("title"): score += 1
                if l.get("linkedin_url"): score += 1
                if l.get("phone"): score += 1
                if l.get("company_domain"): score += 1
                if l.get("employee_count"): score += 1
                if l.get("industry"): score += 1
                return score

            # Sort by completeness
            group_leads.sort(key=completeness_score, reverse=True)

            # Primary is most complete
            primary = group_leads[0].copy()

            # Merge in missing data from others
            for other in group_leads[1:]:
                for key, value in other.items():
                    if value and not primary.get(key):
                        primary[key] = value

            # Mark as merged
            primary["_duplicate_count"] = len(group_leads)
            primary["_duplicate_group"] = group_id

            merged_leads.append(primary)
            processed_groups.add(group_id)
        else:
            # Not a duplicate, keep as-is
            merged_leads.append(lead.copy())

    return merged_leads


def deduplicate_companies(
    input_path: str,
    output_path: str,
    threshold: float = 0.85
) -> Dict:
    """
    Main deduplication function.

    Args:
        input_path: Path to normalized leads JSON
        output_path: Path for deduplicated output
        threshold: Fuzzy match threshold (0-1)

    Returns:
        Summary dict with statistics
    """
    # Load leads
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    leads = data.get("leads", [])
    original_count = len(leads)

    print(f"Loaded {original_count} leads")

    # Find duplicates
    print(f"Finding duplicates (threshold={threshold})...")
    groups, lead_to_group = find_duplicates(leads, threshold)

    duplicate_count = sum(len(g) - 1 for g in groups.values())
    groups_count = len(groups)

    print(f"Found {groups_count} duplicate groups ({duplicate_count} duplicates)")

    # Merge duplicates
    print("Merging duplicates...")
    merged_leads = merge_duplicates(leads, groups, lead_to_group)

    # Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    output_data = {
        "deduplication_summary": {
            "original_count": original_count,
            "deduplicated_count": len(merged_leads),
            "duplicates_removed": original_count - len(merged_leads),
            "duplicate_groups": groups_count,
            "threshold_used": threshold
        },
        "leads": merged_leads
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nSaved {len(merged_leads)} deduplicated leads to: {output_path}")

    return output_data["deduplication_summary"]


def main():
    parser = argparse.ArgumentParser(description="Deduplicate companies in lead data")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/deduped.json")
    parser.add_argument("--threshold", "-t", type=float, default=0.85,
                        help="Fuzzy match threshold (0-1, default: 0.85)")

    args = parser.parse_args()

    summary = deduplicate_companies(args.input, args.output, args.threshold)

    print("\nSummary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
