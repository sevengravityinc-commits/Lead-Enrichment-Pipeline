"""
TAM Output Generator

Exports scored leads to various formats:
- Google Sheets (primary)
- Excel
- CSV
- SmartLead-ready format

Usage:
    python output_tam.py .tmp/leads/final_scored.json --config configs/example_marketing.json
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import pandas as pd

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from google_sheets_helper import create_spreadsheet, write_to_sheet
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False


# Output column mapping
OUTPUT_COLUMNS = {
    # Contact info
    "email": "Email",
    "first_name": "First Name",
    "last_name": "Last Name",
    "full_name": "Full Name",
    "title": "Title",
    "linkedin_url": "LinkedIn URL",
    "phone": "Phone",

    # Company info
    "company_name": "Company",
    "company_domain": "Domain",
    "company_website": "Website",
    "industry": "Industry",
    "employee_count": "Employees",
    "location_city": "City",
    "location_state": "State",
    "location_country": "Country",

    # Scores
    "_quality_score": "Quality Score",
    "_quality_tier": "Tier",
    "_icp_score": "ICP Score",
    "_icp_tier": "ICP Tier",
    "_seniority_level": "Seniority",

    # Status
    "_campaign_ready": "Campaign Ready",
    "_email_verified": "Email Verified",
}

# SmartLead column mapping
SMARTLEAD_COLUMNS = {
    "email": "email",
    "first_name": "first_name",
    "last_name": "last_name",
    "company_name": "company_name",
    "title": "custom1",
    "linkedin_url": "custom2",
    "phone": "phone_number",
    "company_domain": "custom3",
    "industry": "custom4",
    "_quality_score": "custom5",
}


def prepare_output_data(
    leads: List[Dict],
    include_reasoning: bool = False,
    min_quality_score: int = 0
) -> List[Dict]:
    """
    Prepare leads for output with selected columns.

    Args:
        leads: List of lead dicts
        include_reasoning: Include AI reasoning columns
        min_quality_score: Minimum score to include

    Returns:
        List of output-ready dicts
    """
    output = []

    for lead in leads:
        # Filter by score
        score = lead.get("_quality_score", 0)
        if score < min_quality_score:
            continue

        # Map to output columns
        row = {}
        for key, header in OUTPUT_COLUMNS.items():
            value = lead.get(key)
            if value is not None:
                row[header] = value

        # Include reasoning if requested
        if include_reasoning:
            if lead.get("_icp_reasoning"):
                row["ICP Reasoning"] = "; ".join(lead["_icp_reasoning"])
            if lead.get("_filter_reason"):
                row["Filter Reason"] = lead["_filter_reason"]

        output.append(row)

    return output


def prepare_smartlead_data(leads: List[Dict]) -> List[Dict]:
    """
    Prepare leads for SmartLead upload format.

    Args:
        leads: List of lead dicts

    Returns:
        List of SmartLead-formatted dicts
    """
    output = []

    for lead in leads:
        # Only include campaign-ready leads
        if not lead.get("_campaign_ready"):
            continue

        # Must have email
        if not lead.get("email"):
            continue

        row = {}
        for key, header in SMARTLEAD_COLUMNS.items():
            value = lead.get(key)
            if value is not None:
                row[header] = str(value) if value else ""

        output.append(row)

    return output


def output_to_excel(
    leads: List[Dict],
    output_path: str,
    include_reasoning: bool = False,
    min_quality_score: int = 0
) -> str:
    """Export leads to Excel file"""
    data = prepare_output_data(leads, include_reasoning, min_quality_score)

    if not data:
        print("No leads to export")
        return None

    df = pd.DataFrame(data)
    df.to_excel(output_path, index=False, sheet_name="TAM")

    print(f"Exported {len(data)} leads to Excel: {output_path}")
    return output_path


def output_to_csv(
    leads: List[Dict],
    output_path: str,
    smartlead_format: bool = False
) -> str:
    """Export leads to CSV file"""
    if smartlead_format:
        data = prepare_smartlead_data(leads)
    else:
        data = prepare_output_data(leads)

    if not data:
        print("No leads to export")
        return None

    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)

    print(f"Exported {len(data)} leads to CSV: {output_path}")
    return output_path


def output_to_google_sheets(
    leads: List[Dict],
    sheet_name: str,
    include_reasoning: bool = False,
    min_quality_score: int = 0
) -> Optional[str]:
    """Export leads to Google Sheets"""
    if not SHEETS_AVAILABLE:
        print("Google Sheets helper not available")
        return None

    data = prepare_output_data(leads, include_reasoning, min_quality_score)

    if not data:
        print("No leads to export")
        return None

    # Create spreadsheet
    try:
        spreadsheet_id = create_spreadsheet(sheet_name)

        # Convert to list of lists for sheets API
        headers = list(data[0].keys())
        rows = [headers]
        for row in data:
            rows.append([row.get(h, "") for h in headers])

        # Write data
        write_to_sheet(spreadsheet_id, "Sheet1", rows)

        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        print(f"Exported {len(data)} leads to Google Sheets: {url}")
        return url

    except Exception as e:
        print(f"Error exporting to Google Sheets: {e}")
        return None


def generate_summary_report(leads: List[Dict], config: Dict) -> Dict:
    """Generate summary statistics for the TAM output"""
    total = len(leads)

    # Score distribution
    tier_a = sum(1 for l in leads if l.get("_quality_tier") == "A")
    tier_b = sum(1 for l in leads if l.get("_quality_tier") == "B")
    tier_c = sum(1 for l in leads if l.get("_quality_tier") == "C")
    tier_d = sum(1 for l in leads if l.get("_quality_tier") == "D")

    # Campaign readiness
    campaign_ready = sum(1 for l in leads if l.get("_campaign_ready"))
    has_email = sum(1 for l in leads if l.get("email"))
    verified_email = sum(1 for l in leads if l.get("_email_verified"))

    # Seniority distribution
    seniority_counts = {}
    for lead in leads:
        s = lead.get("_seniority_level", "unknown")
        seniority_counts[s] = seniority_counts.get(s, 0) + 1

    return {
        "total_leads": total,
        "tier_distribution": {
            "A (80+)": tier_a,
            "B (60-79)": tier_b,
            "C (40-59)": tier_c,
            "D (<40)": tier_d
        },
        "campaign_ready": campaign_ready,
        "has_email": has_email,
        "verified_email": verified_email,
        "seniority_distribution": seniority_counts,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


def output_tam(
    input_path: str,
    config: Dict[str, Any],
    output_dir: str = ".tmp/output"
) -> Dict:
    """
    Generate TAM output in configured format.

    Args:
        input_path: Path to final scored leads JSON
        config: Campaign config
        output_dir: Directory for output files

    Returns:
        Summary with output paths
    """
    # Load input
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    leads = data.get("leads", [])
    total = len(leads)

    print(f"Generating TAM output for {total} leads...")

    # Get output config
    output_config = config.get("output", {})
    output_format = output_config.get("format", "excel")
    sheet_name = output_config.get("sheet_name", f"TAM {datetime.now().strftime('%Y-%m-%d')}")
    min_quality = output_config.get("min_quality_score", 0)
    include_reasoning = output_config.get("include_reasoning", False)
    smartlead_ready = output_config.get("smartlead_ready", True)

    # Ensure output directory
    os.makedirs(output_dir, exist_ok=True)

    # Generate outputs
    outputs = {}
    campaign_id = config.get("campaign_id", "campaign")

    # Primary output
    if output_format == "google_sheets":
        url = output_to_google_sheets(leads, sheet_name, include_reasoning, min_quality)
        if url:
            outputs["google_sheets"] = url
        else:
            # Fallback to Excel
            excel_path = os.path.join(output_dir, f"{campaign_id}_tam.xlsx")
            output_to_excel(leads, excel_path, include_reasoning, min_quality)
            outputs["excel"] = excel_path

    elif output_format == "excel":
        excel_path = os.path.join(output_dir, f"{campaign_id}_tam.xlsx")
        output_to_excel(leads, excel_path, include_reasoning, min_quality)
        outputs["excel"] = excel_path

    elif output_format == "csv":
        csv_path = os.path.join(output_dir, f"{campaign_id}_tam.csv")
        output_to_csv(leads, csv_path)
        outputs["csv"] = csv_path

    # SmartLead-ready CSV (always generate if enabled)
    if smartlead_ready:
        smartlead_path = os.path.join(output_dir, f"{campaign_id}_smartlead.csv")
        output_to_csv(leads, smartlead_path, smartlead_format=True)
        outputs["smartlead_csv"] = smartlead_path

    # Generate summary
    summary = generate_summary_report(leads, config)
    summary["outputs"] = outputs

    # Save summary
    summary_path = os.path.join(output_dir, f"{campaign_id}_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\nTAM Output Generated:")
    for key, path in outputs.items():
        print(f"  {key}: {path}")
    print(f"  summary: {summary_path}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Generate TAM output")
    parser.add_argument("input", help="Input JSON file path (final scored leads)")
    parser.add_argument("--config", "-c", required=True, help="Campaign config file")
    parser.add_argument("--output-dir", "-o", help="Output directory",
                        default=".tmp/output")

    args = parser.parse_args()

    # Load config
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    summary = output_tam(args.input, config, args.output_dir)

    print("\nSummary:")
    print(f"  Total leads: {summary['total_leads']}")
    print(f"  Campaign ready: {summary['campaign_ready']}")
    print(f"  Tier distribution: {summary['tier_distribution']}")


if __name__ == "__main__":
    main()
