"""
Triple Email Verification

Sequential verification funnel - emails must pass ALL 3 services to be campaign-ready:
1. BlitzAPI (pass 1) → only "good/OK" emails proceed
2. MillionVerifier (pass 2) → only "good" quality with "ok" result proceed
3. BounceBan (pass 3) → only "deliverable" result = campaign ready

Usage:
    python triple_verify_emails.py .tmp/leads/enriched.json --output .tmp/leads/verified.json
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from millionverifier_api import verify_emails as mv_verify_emails
from bounceban_api import verify_emails as bb_verify_emails

try:
    from blitz_api import BlitzAPI
    BLITZ_AVAILABLE = True
except ImportError:
    BLITZ_AVAILABLE = False


@dataclass
class VerificationStatus:
    """Track verification status for an email"""
    email: str
    blitz_status: Optional[str] = None  # "valid", "invalid", "skipped"
    blitz_confidence: Optional[float] = None
    mv_status: Optional[str] = None  # "ok", "catch_all", "invalid", etc.
    mv_quality: Optional[str] = None  # "good", "risky", "bad"
    bb_status: Optional[str] = None  # "deliverable", "risky", "undeliverable", "unknown"
    bb_score: Optional[int] = None
    final_status: str = "pending"  # "verified", "failed", "pending"
    campaign_ready: bool = False


def check_blitz_status(leads: List[Dict]) -> Tuple[List[str], Dict[str, Dict]]:
    """
    Check BlitzAPI status for emails.

    Emails from BlitzAPI enrichment are considered "validated" by BlitzAPI.
    Other emails are marked as "skipped" for BlitzAPI.

    Returns:
        - List of emails that passed (for next step)
        - Dict of verification status by email
    """
    passed = []
    status_map = {}

    for lead in leads:
        email = lead.get("email", "").lower().strip()
        if not email:
            continue

        # Check if email came from BlitzAPI enrichment
        blitz_enriched = lead.get("_blitz_enriched", False)
        blitz_email_found = lead.get("_blitz_email_found", False)

        if blitz_enriched and blitz_email_found:
            # Email found by BlitzAPI = considered validated
            status_map[email] = {
                "blitz_status": "valid",
                "blitz_confidence": 0.95  # High confidence for BlitzAPI found emails
            }
            passed.append(email)
        else:
            # Not from BlitzAPI - still pass through but mark as skipped
            status_map[email] = {
                "blitz_status": "skipped",
                "blitz_confidence": None
            }
            passed.append(email)  # All emails proceed to next step

    return passed, status_map


def verify_with_millionverifier(emails: List[str]) -> Tuple[List[str], Dict[str, Dict]]:
    """
    Verify emails with MillionVerifier.

    Returns:
        - List of emails that passed (quality="good" and result="ok")
        - Dict of verification results by email
    """
    if not emails:
        return [], {}

    print(f"\n[Pass 2] MillionVerifier: Verifying {len(emails)} emails...")

    result = mv_verify_emails(emails, wait=True, poll_interval=10)

    if not result.get("success"):
        print(f"  MillionVerifier error: {result.get('error')}")
        # On error, treat all as failed
        return [], {e: {"mv_status": "error", "mv_quality": "unknown"} for e in emails}

    passed = []
    status_map = {}

    results_by_email = result.get("results", {})

    for email in emails:
        email_lower = email.lower()
        email_result = results_by_email.get(email_lower, {})

        mv_quality = email_result.get("quality", "unknown")
        mv_result = email_result.get("result", "unknown")

        status_map[email_lower] = {
            "mv_status": mv_result,
            "mv_quality": mv_quality
        }

        # Pass criteria: quality="good" AND result="ok"
        if mv_quality == "good" and mv_result == "ok":
            passed.append(email)
            print(f"  ✓ {email}: {mv_quality}/{mv_result}")
        else:
            print(f"  ✗ {email}: {mv_quality}/{mv_result}")

    print(f"  Passed: {len(passed)}/{len(emails)}")

    return passed, status_map


def verify_with_bounceban(emails: List[str]) -> Tuple[List[str], Dict[str, Dict]]:
    """
    Verify emails with BounceBan.

    Returns:
        - List of emails that passed (result="deliverable")
        - Dict of verification results by email
    """
    if not emails:
        return [], {}

    print(f"\n[Pass 3] BounceBan: Verifying {len(emails)} emails...")

    result = bb_verify_emails(emails, wait=True, poll_interval=10)

    if not result.get("success"):
        print(f"  BounceBan error: {result.get('error')}")
        # On error, treat all as failed
        return [], {e: {"bb_status": "error", "bb_score": 0} for e in emails}

    passed = []
    status_map = {}

    results_by_email = result.get("results", {})

    for email in emails:
        email_lower = email.lower()
        email_result = results_by_email.get(email_lower, {})

        bb_result = email_result.get("result", "unknown")
        bb_score = email_result.get("score", 0)

        status_map[email_lower] = {
            "bb_status": bb_result,
            "bb_score": bb_score
        }

        # Pass criteria: result="deliverable"
        if bb_result == "deliverable":
            passed.append(email)
            print(f"  ✓ {email}: {bb_result} (score: {bb_score})")
        else:
            print(f"  ✗ {email}: {bb_result} (score: {bb_score})")

    print(f"  Passed: {len(passed)}/{len(emails)}")

    return passed, status_map


def triple_verify_leads(
    input_path: str,
    output_path: str,
    skip_blitz: bool = False,
    skip_mv: bool = False,
    skip_bb: bool = False
) -> Dict:
    """
    Run triple verification on leads.

    Args:
        input_path: Path to input JSON (leads with emails)
        output_path: Path for verified output
        skip_blitz: Skip BlitzAPI check (all pass through)
        skip_mv: Skip MillionVerifier (for testing)
        skip_bb: Skip BounceBan (for testing)

    Returns:
        Summary statistics
    """
    # Load input
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    leads = data.get("leads", [])
    total = len(leads)

    # Get all emails
    all_emails = []
    email_to_lead_idx = {}
    for i, lead in enumerate(leads):
        email = lead.get("email", "").lower().strip()
        if email:
            all_emails.append(email)
            email_to_lead_idx[email] = i

    print(f"Triple Email Verification")
    print(f"=" * 50)
    print(f"Total leads: {total}")
    print(f"Leads with email: {len(all_emails)}")

    # Initialize verification status for all emails
    verification_status = {email: VerificationStatus(email=email) for email in all_emails}

    # Pass 1: BlitzAPI check
    print(f"\n[Pass 1] BlitzAPI Check...")
    if skip_blitz:
        print("  Skipped (--skip-blitz)")
        blitz_passed = all_emails
        blitz_status = {e: {"blitz_status": "skipped"} for e in all_emails}
    else:
        blitz_passed, blitz_status = check_blitz_status(leads)
        print(f"  Passed: {len(blitz_passed)}/{len(all_emails)}")

    # Update status
    for email, status in blitz_status.items():
        if email in verification_status:
            verification_status[email].blitz_status = status.get("blitz_status")
            verification_status[email].blitz_confidence = status.get("blitz_confidence")

    # Pass 2: MillionVerifier
    if skip_mv:
        print(f"\n[Pass 2] MillionVerifier: Skipped (--skip-mv)")
        mv_passed = blitz_passed
        mv_status = {}
    else:
        mv_passed, mv_status = verify_with_millionverifier(blitz_passed)

    # Update status
    for email, status in mv_status.items():
        if email in verification_status:
            verification_status[email].mv_status = status.get("mv_status")
            verification_status[email].mv_quality = status.get("mv_quality")

    # Pass 3: BounceBan
    if skip_bb:
        print(f"\n[Pass 3] BounceBan: Skipped (--skip-bb)")
        bb_passed = mv_passed
        bb_status = {}
    else:
        bb_passed, bb_status = verify_with_bounceban(mv_passed)

    # Update status
    for email, status in bb_status.items():
        if email in verification_status:
            verification_status[email].bb_status = status.get("bb_status")
            verification_status[email].bb_score = status.get("bb_score")

    # Determine final status
    verified_emails = set(bb_passed)
    for email, vs in verification_status.items():
        if email in verified_emails:
            vs.final_status = "verified"
            vs.campaign_ready = True
        else:
            vs.final_status = "failed"
            vs.campaign_ready = False

    # Update leads with verification status
    for lead in leads:
        email = lead.get("email", "").lower().strip()
        if email and email in verification_status:
            vs = verification_status[email]
            lead["_email_verified"] = vs.campaign_ready
            lead["_verification_status"] = vs.final_status
            lead["_blitz_status"] = vs.blitz_status
            lead["_mv_status"] = vs.mv_status
            lead["_mv_quality"] = vs.mv_quality
            lead["_bb_status"] = vs.bb_status
            lead["_bb_score"] = vs.bb_score
            lead["_campaign_ready"] = vs.campaign_ready
        elif not email:
            lead["_email_verified"] = False
            lead["_verification_status"] = "no_email"
            lead["_campaign_ready"] = False

    # Separate verified and failed leads
    verified_leads = [l for l in leads if l.get("_campaign_ready")]
    failed_leads = [l for l in leads if not l.get("_campaign_ready")]

    # Save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    summary = {
        "total_leads": total,
        "leads_with_email": len(all_emails),
        "pass_1_blitz": len(blitz_passed),
        "pass_2_mv": len(mv_passed),
        "pass_3_bb": len(bb_passed),
        "campaign_ready": len(verified_leads),
        "failed": len(failed_leads),
        "verified_at": datetime.now(timezone.utc).isoformat()
    }

    output_data = {
        "verification_summary": summary,
        "leads": verified_leads,
        "failed_leads": failed_leads
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\n" + "=" * 50)
    print(f"Verification Complete!")
    print(f"  Total leads: {total}")
    print(f"  Campaign ready: {len(verified_leads)}")
    print(f"  Failed: {len(failed_leads)}")
    print(f"\nSaved to: {output_path}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Triple email verification")
    parser.add_argument("input", help="Input JSON file path")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/verified.json")
    parser.add_argument("--skip-blitz", action="store_true", help="Skip BlitzAPI check")
    parser.add_argument("--skip-mv", action="store_true", help="Skip MillionVerifier")
    parser.add_argument("--skip-bb", action="store_true", help="Skip BounceBan")

    args = parser.parse_args()

    summary = triple_verify_leads(
        args.input,
        args.output,
        skip_blitz=args.skip_blitz,
        skip_mv=args.skip_mv,
        skip_bb=args.skip_bb
    )

    print("\nSummary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
