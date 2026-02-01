"""
BlitzAPI Lead Enrichment

Enriches leads with decision-makers using BlitzAPI:
- Finds 1-4 decision-makers per company (based on size)
- Enriches with verified emails
- Re-verifies existing contacts

Uses tiered contact strategy:
- 1-20 employees: 1-2 contacts (CEO/Founder)
- 21-100 employees: 2-3 contacts (CEO + VPs)
- 100+ employees: 3-4 contacts (VPs + Directors)

Usage:
    python blitz_enrich_leads.py .tmp/leads/scored.json --config configs/example_marketing.json
"""

import os
import sys
import json
import argparse
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from blitz_api import BlitzAPI, BlitzAPIError
from checkpoint_manager import CheckpointManager


def get_contact_target(employee_count: Optional[int], config: Dict) -> int:
    """
    Determine number of contacts to find based on company size.

    Default:
    - 1-20 employees: 2 contacts
    - 21-100 employees: 3 contacts
    - 100+ employees: 4 contacts
    """
    contacts_config = config.get("target_roles", {}).get("contacts_by_company_size", {})

    small = contacts_config.get("small", {"max_employees": 20, "contacts": 2})
    medium = contacts_config.get("medium", {"max_employees": 100, "contacts": 3})
    large = contacts_config.get("large", {"min_employees": 100, "contacts": 4})

    if employee_count is None:
        return 3  # Default

    if employee_count <= small.get("max_employees", 20):
        return small.get("contacts", 2)
    elif employee_count <= medium.get("max_employees", 100):
        return medium.get("contacts", 3)
    else:
        return large.get("contacts", 4)


def enrich_company(
    lead: Dict,
    api: BlitzAPI,
    config: Dict,
    enrich_emails: bool = True
) -> List[Dict]:
    """
    Enrich a single company with decision-makers.

    Args:
        lead: Lead dict with company info
        api: BlitzAPI instance
        config: Campaign config
        enrich_emails: Whether to get emails for contacts

    Returns:
        List of enriched contact dicts
    """
    company_name = lead.get("company_name", "Unknown")
    company_linkedin = lead.get("company_linkedin_url")
    company_domain = lead.get("company_domain")
    employee_count = lead.get("employee_count")

    # Determine how many contacts to find
    max_contacts = get_contact_target(employee_count, config)

    # Get LinkedIn URL from domain if needed
    if not company_linkedin and company_domain:
        try:
            result = api.domain_to_linkedin(company_domain)
            if result.found:
                company_linkedin = result.company_linkedin_url
                lead["company_linkedin_url"] = company_linkedin
        except BlitzAPIError as e:
            print(f"  Warning: Could not get LinkedIn URL for {company_domain}: {e.message}")

    if not company_linkedin:
        print(f"  Skipping {company_name}: No LinkedIn URL available")
        return []

    # Search for decision-makers
    try:
        contacts = api.search_decision_makers(
            company_linkedin_url=company_linkedin,
            company_size=employee_count,
            with_email=enrich_emails
        )

        # Limit to max contacts
        contacts = contacts[:max_contacts]

        print(f"  Found {len(contacts)} decision-makers for {company_name}")

        return contacts

    except BlitzAPIError as e:
        print(f"  Error searching {company_name}: {e.message}")
        return []


def enrich_existing_contact(
    lead: Dict,
    api: BlitzAPI
) -> Dict:
    """
    Re-verify an existing contact's email.

    Args:
        lead: Lead dict with existing contact
        api: BlitzAPI instance

    Returns:
        Updated lead dict
    """
    linkedin_url = lead.get("linkedin_url")
    existing_email = lead.get("email")

    if not linkedin_url:
        return lead

    try:
        result = api.find_work_email(linkedin_url)

        if result.found:
            lead["email"] = result.email
            lead["_email_verified"] = True
            lead["_email_source"] = "blitz_api"

            # Check if email changed
            if existing_email and existing_email.lower() != result.email.lower():
                lead["_email_previous"] = existing_email
                lead["_email_updated"] = True
        else:
            lead["_email_verified"] = False
            lead["_email_source"] = "blitz_api_not_found"

    except BlitzAPIError as e:
        lead["_email_verified"] = False
        lead["_email_error"] = str(e.message)

    return lead


def enrich_leads(
    input_path: str,
    output_path: str,
    config: Dict[str, Any],
    delay: float = 0.5
) -> Dict:
    """
    Enrich all leads with decision-makers via BlitzAPI.

    Args:
        input_path: Path to scored leads JSON
        output_path: Path for enriched output
        config: Campaign config
        delay: Delay between API calls (rate limiting)

    Returns:
        Summary statistics
    """
    # Load input
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    leads = data.get("leads", [])
    total = len(leads)

    print(f"Enriching {total} leads with BlitzAPI...")

    # Check enrichment config
    enrich_config = config.get("enrichment", {}).get("blitz_api", {})

    if not enrich_config.get("enabled", True):
        print("BlitzAPI enrichment disabled in config, skipping...")
        output_data = {
            "enrichment_summary": {"skipped": True, "reason": "disabled in config"},
            "leads": leads
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, default=str)
        return {"skipped": True}

    find_decision_makers = enrich_config.get("find_decision_makers", True)
    enrich_emails = enrich_config.get("enrich_emails", True)

    # Initialize API
    api = BlitzAPI()

    # Check credits
    try:
        key_info = api.get_key_info()
        print(f"BlitzAPI credits available: {key_info.remaining_credits}")
    except BlitzAPIError as e:
        print(f"Error checking BlitzAPI credits: {e.message}")
        return {"error": e.message}

    # Setup checkpoint
    checkpoint = CheckpointManager.for_file(input_path, "blitz_enrich")

    # Check for existing checkpoint
    if checkpoint.has_checkpoint():
        start_idx, enriched_leads, _ = checkpoint.load()
        print(f"Resuming from lead {start_idx}")
    else:
        start_idx = 0
        enriched_leads = []

    # Process leads
    stats = {
        "companies_processed": 0,
        "contacts_found": 0,
        "emails_found": 0,
        "existing_contacts_verified": 0,
        "errors": 0
    }

    for i in range(start_idx, total):
        lead = leads[i]
        company_name = lead.get("company_name", f"Lead {i}")

        print(f"[{i + 1}/{total}] Processing: {company_name}")

        # Check if lead already has contact info
        has_contact = lead.get("email") or lead.get("linkedin_url")

        if has_contact and not find_decision_makers:
            # Just verify existing contact
            if lead.get("linkedin_url") and enrich_emails:
                lead = enrich_existing_contact(lead, api)
                stats["existing_contacts_verified"] += 1

            enriched_leads.append(lead)

        elif find_decision_makers:
            # Find new decision-makers
            contacts = enrich_company(lead, api, config, enrich_emails)

            if contacts:
                # Create a lead entry for each contact
                for contact in contacts:
                    enriched_lead = lead.copy()
                    enriched_lead["first_name"] = contact.get("first_name")
                    enriched_lead["last_name"] = contact.get("last_name")
                    enriched_lead["full_name"] = contact.get("full_name")
                    enriched_lead["title"] = contact.get("title")
                    enriched_lead["linkedin_url"] = contact.get("linkedin_url")
                    enriched_lead["email"] = contact.get("email")
                    enriched_lead["_email_found"] = contact.get("email_found", False)
                    enriched_lead["_icp_rank"] = contact.get("icp_rank")
                    enriched_lead["_enriched_by"] = "blitz_api"
                    enriched_lead["has_contact"] = True
                    enriched_lead["needs_enrichment"] = False

                    enriched_leads.append(enriched_lead)
                    stats["contacts_found"] += 1

                    if contact.get("email"):
                        stats["emails_found"] += 1
            else:
                # No contacts found, keep original lead
                lead["_enrichment_attempted"] = True
                lead["_contacts_found"] = 0
                enriched_leads.append(lead)
                stats["errors"] += 1

            stats["companies_processed"] += 1
        else:
            # Keep as-is
            enriched_leads.append(lead)

        # Rate limiting
        time.sleep(delay)

        # Checkpoint every 50 leads
        if (i + 1) % 50 == 0:
            checkpoint.save(i + 1, enriched_leads)
            print(f"  Checkpoint saved at lead {i + 1}")

    # Clear checkpoint on success
    checkpoint.clear()

    # Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    summary = {
        "input_leads": total,
        "output_leads": len(enriched_leads),
        **stats
    }

    output_data = {
        "enrichment_summary": summary,
        "leads": enriched_leads
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nEnrichment Results:")
    print(f"  Companies processed: {stats['companies_processed']}")
    print(f"  Contacts found: {stats['contacts_found']}")
    print(f"  Emails found: {stats['emails_found']}")
    print(f"  Existing verified: {stats['existing_contacts_verified']}")
    print(f"  Errors: {stats['errors']}")
    print(f"\nSaved {len(enriched_leads)} enriched leads to: {output_path}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Enrich leads with BlitzAPI")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--config", "-c", required=True, help="Campaign config file")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/enriched.json")
    parser.add_argument("--delay", "-d", type=float, default=0.5,
                        help="Delay between API calls (seconds)")

    args = parser.parse_args()

    # Load config
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    summary = enrich_leads(args.input, args.output, config, args.delay)

    print("\nSummary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
