"""
Contact Deduplication Module

Identifies duplicate contacts by:
1. Exact email match
2. Email alias detection (john@, j.doe@, johnd@ at same domain)
3. Name + company match

Usage:
    python deduplicate_contacts.py .tmp/leads/deduped.json --output .tmp/leads/contacts_deduped.json
"""

import os
import sys
import json
import re
import argparse
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict
from difflib import SequenceMatcher


def normalize_email(email: str) -> Optional[str]:
    """Normalize email for comparison"""
    if not email:
        return None

    email = email.lower().strip()

    # Remove dots from local part (gmail style)
    # But only for well-known providers that do this
    local, domain = email.split("@") if "@" in email else (email, "")

    # Remove + aliases (john+test@domain.com -> john@domain.com)
    local = local.split("+")[0]

    return f"{local}@{domain}" if domain else None


def extract_name_parts(email: str) -> Set[str]:
    """Extract potential name parts from email local part"""
    if not email or "@" not in email:
        return set()

    local = email.split("@")[0].lower()

    # Common separators: ., _, -, none
    parts = re.split(r'[._\-]', local)

    # Also try to split camelCase
    camel_parts = re.findall(r'[a-z]+', local)

    # Filter short parts (likely initials)
    all_parts = set(parts + camel_parts)
    return {p for p in all_parts if len(p) >= 2}


def emails_are_aliases(email1: str, email2: str) -> bool:
    """
    Check if two emails are likely aliases for the same person.

    Examples of aliases:
    - john.doe@company.com and jdoe@company.com
    - john@company.com and j.doe@company.com
    """
    if not email1 or not email2:
        return False

    email1 = email1.lower()
    email2 = email2.lower()

    # Must have same domain
    if "@" not in email1 or "@" not in email2:
        return False

    domain1 = email1.split("@")[1]
    domain2 = email2.split("@")[1]

    if domain1 != domain2:
        return False

    # Skip generic domains
    generic_domains = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}
    if domain1 in generic_domains:
        return False

    # Get name parts from both
    parts1 = extract_name_parts(email1)
    parts2 = extract_name_parts(email2)

    if not parts1 or not parts2:
        return False

    # Check for overlap
    overlap = parts1 & parts2
    if len(overlap) >= 1:
        # At least one name part matches
        return True

    # Check if one is initial of the other
    for p1 in parts1:
        for p2 in parts2:
            if p1[0] == p2[0] and (len(p1) == 1 or len(p2) == 1):
                # One is initial, other starts with same letter
                return True

    return False


def names_match(lead1: Dict, lead2: Dict) -> bool:
    """Check if two leads have matching names"""
    # Get full names
    name1 = lead1.get("full_name", "").lower().strip()
    name2 = lead2.get("full_name", "").lower().strip()

    if name1 and name2:
        # Exact match
        if name1 == name2:
            return True

        # Fuzzy match
        similarity = SequenceMatcher(None, name1, name2).ratio()
        if similarity >= 0.85:
            return True

    # Try first + last name
    first1 = lead1.get("first_name", "").lower().strip()
    last1 = lead1.get("last_name", "").lower().strip()
    first2 = lead2.get("first_name", "").lower().strip()
    last2 = lead2.get("last_name", "").lower().strip()

    if first1 and last1 and first2 and last2:
        if first1 == first2 and last1 == last2:
            return True

        # First name matches and last name starts with same letter
        if first1 == first2 and last1 and last2 and last1[0] == last2[0]:
            return True

    return False


def same_company(lead1: Dict, lead2: Dict) -> bool:
    """Check if two leads are from the same company"""
    # Check domain
    domain1 = lead1.get("company_domain", "").lower()
    domain2 = lead2.get("company_domain", "").lower()

    if domain1 and domain2 and domain1 == domain2:
        return True

    # Check email domain
    email1 = lead1.get("email", "")
    email2 = lead2.get("email", "")

    if email1 and email2:
        e_domain1 = email1.split("@")[1].lower() if "@" in email1 else ""
        e_domain2 = email2.split("@")[1].lower() if "@" in email2 else ""

        generic = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}
        if e_domain1 and e_domain2 and e_domain1 == e_domain2 and e_domain1 not in generic:
            return True

    # Check company name
    company1 = lead1.get("company_name", "").lower().strip()
    company2 = lead2.get("company_name", "").lower().strip()

    if company1 and company2 and company1 == company2:
        return True

    return False


def find_contact_duplicates(leads: List[Dict]) -> Tuple[Dict[str, List[int]], Dict[int, str]]:
    """
    Find duplicate contacts in lead list.

    Returns:
        - groups: Dict mapping group_id to list of lead indices
        - lead_to_group: Dict mapping lead index to group_id
    """
    n = len(leads)
    groups = {}
    lead_to_group = {}
    group_counter = 0

    # First pass: exact email match
    email_groups = defaultdict(list)
    for i, lead in enumerate(leads):
        email = normalize_email(lead.get("email", ""))
        if email:
            email_groups[email].append(i)

    for email, indices in email_groups.items():
        if len(indices) > 1:
            group_id = f"email_{group_counter}"
            group_counter += 1
            groups[group_id] = indices
            for idx in indices:
                lead_to_group[idx] = group_id

    # Second pass: email aliases
    ungrouped = [i for i in range(n) if i not in lead_to_group]

    # Group by domain for efficiency
    domain_leads = defaultdict(list)
    for i in ungrouped:
        email = leads[i].get("email", "")
        if email and "@" in email:
            domain = email.split("@")[1].lower()
            domain_leads[domain].append(i)

    # Check aliases within each domain
    for domain, indices in domain_leads.items():
        if len(indices) < 2:
            continue

        # Skip generic domains
        if domain in {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}:
            continue

        processed = set()
        for i in indices:
            if i in processed or i in lead_to_group:
                continue

            email_i = leads[i].get("email", "")
            matches = [i]

            for j in indices:
                if j <= i or j in processed or j in lead_to_group:
                    continue

                email_j = leads[j].get("email", "")

                if emails_are_aliases(email_i, email_j):
                    matches.append(j)
                    processed.add(j)

            if len(matches) > 1:
                group_id = f"alias_{group_counter}"
                group_counter += 1
                groups[group_id] = matches
                for idx in matches:
                    lead_to_group[idx] = group_id
                    processed.add(idx)

    # Third pass: name + company match (no email)
    ungrouped = [i for i in range(n) if i not in lead_to_group]

    for i in ungrouped:
        if i in lead_to_group:
            continue

        lead_i = leads[i]
        if not lead_i.get("full_name") and not (lead_i.get("first_name") and lead_i.get("last_name")):
            continue

        matches = [i]

        for j in ungrouped:
            if j <= i or j in lead_to_group:
                continue

            lead_j = leads[j]

            if same_company(lead_i, lead_j) and names_match(lead_i, lead_j):
                matches.append(j)

        if len(matches) > 1:
            group_id = f"name_{group_counter}"
            group_counter += 1
            groups[group_id] = matches
            for idx in matches:
                lead_to_group[idx] = group_id

    return groups, lead_to_group


def merge_contact_duplicates(
    leads: List[Dict],
    groups: Dict[str, List[int]],
    lead_to_group: Dict[int, str]
) -> List[Dict]:
    """Merge duplicate contacts, keeping most complete data"""
    merged_leads = []
    processed_groups = set()

    for i, lead in enumerate(leads):
        group_id = lead_to_group.get(i)

        if group_id:
            if group_id in processed_groups:
                continue

            group_indices = groups[group_id]
            group_leads = [leads[idx] for idx in group_indices]

            # Score by completeness
            def score(l):
                s = 0
                if l.get("email"): s += 3
                if l.get("first_name"): s += 1
                if l.get("last_name"): s += 1
                if l.get("title"): s += 2
                if l.get("linkedin_url"): s += 2
                if l.get("phone"): s += 1
                return s

            group_leads.sort(key=score, reverse=True)
            primary = group_leads[0].copy()

            # Merge missing data
            for other in group_leads[1:]:
                for key, value in other.items():
                    if value and not primary.get(key):
                        primary[key] = value

            primary["_contact_duplicate_count"] = len(group_leads)
            primary["_contact_duplicate_group"] = group_id

            merged_leads.append(primary)
            processed_groups.add(group_id)
        else:
            merged_leads.append(lead.copy())

    return merged_leads


def deduplicate_contacts(input_path: str, output_path: str) -> Dict:
    """
    Main contact deduplication function.

    Returns:
        Summary dict with statistics
    """
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    leads = data.get("leads", [])
    original_count = len(leads)

    print(f"Loaded {original_count} leads")

    # Find duplicates
    print("Finding contact duplicates...")
    groups, lead_to_group = find_contact_duplicates(leads)

    duplicate_count = sum(len(g) - 1 for g in groups.values())
    groups_count = len(groups)

    print(f"Found {groups_count} duplicate groups ({duplicate_count} duplicates)")

    # Merge
    print("Merging duplicates...")
    merged_leads = merge_contact_duplicates(leads, groups, lead_to_group)

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    output_data = {
        "contact_deduplication_summary": {
            "original_count": original_count,
            "deduplicated_count": len(merged_leads),
            "duplicates_removed": original_count - len(merged_leads),
            "duplicate_groups": groups_count
        },
        "leads": merged_leads
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nSaved {len(merged_leads)} deduplicated leads to: {output_path}")

    return output_data["contact_deduplication_summary"]


def main():
    parser = argparse.ArgumentParser(description="Deduplicate contacts in lead data")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/contacts_deduped.json")

    args = parser.parse_args()

    summary = deduplicate_contacts(args.input, args.output)

    print("\nSummary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
