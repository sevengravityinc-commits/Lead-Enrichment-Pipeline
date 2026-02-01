"""
Lead Ingestion Module

Normalizes lead data from various sources (Excel, CSV, Google Sheets)
into a unified schema for downstream processing.

Usage:
    python lead_ingest.py "leads.xlsx" --output .tmp/leads/normalized.json
    python lead_ingest.py "leads.csv" --output .tmp/leads/normalized.json
"""

import os
import sys
import json
import uuid
import argparse
import re
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from pathlib import Path

import pandas as pd


@dataclass
class Lead:
    """Normalized lead schema"""
    # Core identifiers
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    email: Optional[str] = None

    # Contact info
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    title: Optional[str] = None
    linkedin_url: Optional[str] = None
    phone: Optional[str] = None

    # Company info
    company_name: Optional[str] = None
    clean_company_name: Optional[str] = None
    company_domain: Optional[str] = None
    company_website: Optional[str] = None
    company_linkedin_url: Optional[str] = None

    # Firmographics
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    employee_count: Optional[int] = None
    revenue_range: Optional[str] = None
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    location_country: Optional[str] = None

    # Enrichment flags
    has_contact: bool = False
    needs_enrichment: bool = True

    # Source tracking
    source: str = "unknown"
    source_file: Optional[str] = None
    ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # Raw data for reference
    raw_data: Optional[Dict] = None


# Column name mappings for common variations
COLUMN_MAPPINGS = {
    # Email columns
    "email": ["email", "e-mail", "email address", "work email", "business email", "contact email"],
    "first_name": ["first name", "firstname", "first", "given name", "contact first name"],
    "last_name": ["last name", "lastname", "last", "surname", "family name", "contact last name"],
    "full_name": ["full name", "name", "contact name", "person name"],
    "title": ["title", "job title", "position", "role", "headline", "job role"],
    "linkedin_url": ["linkedin", "linkedin url", "linkedin profile", "person linkedin", "contact linkedin"],
    "phone": ["phone", "phone number", "mobile", "cell", "telephone", "direct dial", "work phone"],

    # Company columns
    "company_name": ["company", "company name", "organization", "business name", "account name"],
    "company_domain": ["domain", "company domain", "website domain", "web domain"],
    "company_website": ["website", "company website", "url", "company url", "web address"],
    "company_linkedin_url": ["company linkedin", "company linkedin url", "organization linkedin"],

    # Firmographics
    "industry": ["industry", "sector", "vertical", "market"],
    "sub_industry": ["sub-industry", "sub industry", "niche", "specialty"],
    "employee_count": ["employees", "employee count", "company size", "headcount", "# employees", "size"],
    "revenue_range": ["revenue", "annual revenue", "revenue range", "company revenue"],

    # Location
    "location_city": ["city", "location city", "headquarters city"],
    "location_state": ["state", "region", "province", "state/region"],
    "location_country": ["country", "location country", "headquarters country"],
}


def find_column(df: pd.DataFrame, target: str) -> Optional[str]:
    """
    Find a column in the dataframe that matches the target field.
    Returns the actual column name if found, None otherwise.
    """
    # Get lowercase column names
    columns_lower = {col.lower().strip(): col for col in df.columns}

    # Check direct matches
    mappings = COLUMN_MAPPINGS.get(target, [target])
    for mapping in mappings:
        if mapping.lower() in columns_lower:
            return columns_lower[mapping.lower()]

    # Check partial matches
    for mapping in mappings:
        for col_lower, col_original in columns_lower.items():
            if mapping.lower() in col_lower:
                return col_original

    return None


def extract_domain_from_email(email: str) -> Optional[str]:
    """Extract domain from email address"""
    if not email or "@" not in email:
        return None
    return email.split("@")[1].lower()


def extract_domain_from_url(url: str) -> Optional[str]:
    """Extract domain from URL"""
    if not url:
        return None
    # Remove protocol
    url = re.sub(r'^https?://', '', url.lower())
    # Remove www
    url = re.sub(r'^www\.', '', url)
    # Get domain part
    domain = url.split('/')[0]
    return domain if domain else None


def clean_phone(phone: str) -> Optional[str]:
    """Clean and normalize phone number"""
    if not phone:
        return None
    # Remove non-numeric characters except + and x (for extensions)
    phone = str(phone)
    cleaned = re.sub(r'[^\d+x]', '', phone)
    return cleaned if len(cleaned) >= 7 else None


def parse_employee_count(value: Any) -> Optional[int]:
    """Parse employee count from various formats"""
    if value is None or pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        return int(value) if not pd.isna(value) else None

    value = str(value).strip().lower()

    # Handle ranges like "10-50", "50-200"
    range_match = re.search(r'(\d+)\s*[-–]\s*(\d+)', value)
    if range_match:
        low = int(range_match.group(1))
        high = int(range_match.group(2))
        return (low + high) // 2  # Return midpoint

    # Handle "10+" style
    plus_match = re.search(r'(\d+)\+', value)
    if plus_match:
        return int(plus_match.group(1))

    # Try direct parse
    try:
        # Remove commas and parse
        cleaned = re.sub(r'[,\s]', '', value)
        num_match = re.search(r'\d+', cleaned)
        if num_match:
            return int(num_match.group())
    except:
        pass

    return None


def normalize_linkedin_url(url: str) -> Optional[str]:
    """Normalize LinkedIn URL to standard format"""
    if not url:
        return None

    url = str(url).strip()

    # Check if it's a LinkedIn URL
    if "linkedin.com" not in url.lower():
        return None

    # Ensure https://
    if not url.startswith("http"):
        url = "https://" + url

    # Normalize to https
    url = re.sub(r'^http://', 'https://', url)

    # Remove trailing slashes
    url = url.rstrip('/')

    return url


def ingest_excel(file_path: str) -> List[Lead]:
    """Ingest leads from Excel file"""
    df = pd.read_excel(file_path, dtype=str)
    return _process_dataframe(df, "excel", file_path)


def ingest_csv(file_path: str) -> List[Lead]:
    """Ingest leads from CSV file"""
    # Try different encodings
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(file_path, dtype=str, encoding=encoding)
            return _process_dataframe(df, "csv", file_path)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not read CSV file with supported encodings: {file_path}")


def _process_dataframe(df: pd.DataFrame, source: str, source_file: str) -> List[Lead]:
    """Process dataframe into normalized leads"""
    leads = []

    # Find columns
    col_map = {}
    for target in COLUMN_MAPPINGS.keys():
        found = find_column(df, target)
        if found:
            col_map[target] = found

    print(f"Found columns: {list(col_map.keys())}")

    for idx, row in df.iterrows():
        try:
            # Extract values using column map
            def get_val(field):
                col = col_map.get(field)
                if col and col in row:
                    val = row[col]
                    if pd.isna(val):
                        return None
                    return str(val).strip() if val else None
                return None

            # Create lead
            lead = Lead(
                source=source,
                source_file=os.path.basename(source_file),
                raw_data=row.to_dict()
            )

            # Contact info
            lead.email = get_val("email")
            lead.first_name = get_val("first_name")
            lead.last_name = get_val("last_name")
            lead.full_name = get_val("full_name")
            lead.title = get_val("title")
            lead.linkedin_url = normalize_linkedin_url(get_val("linkedin_url"))
            lead.phone = clean_phone(get_val("phone"))

            # Build full name if not present
            if not lead.full_name and (lead.first_name or lead.last_name):
                parts = [lead.first_name, lead.last_name]
                lead.full_name = " ".join(p for p in parts if p)

            # Company info
            lead.company_name = get_val("company_name")
            lead.company_domain = get_val("company_domain")
            lead.company_website = get_val("company_website")
            lead.company_linkedin_url = normalize_linkedin_url(get_val("company_linkedin_url"))

            # Extract domain from email if not present
            if not lead.company_domain and lead.email:
                lead.company_domain = extract_domain_from_email(lead.email)

            # Extract domain from website if not present
            if not lead.company_domain and lead.company_website:
                lead.company_domain = extract_domain_from_url(lead.company_website)

            # Firmographics
            lead.industry = get_val("industry")
            lead.sub_industry = get_val("sub_industry")
            lead.employee_count = parse_employee_count(get_val("employee_count"))
            lead.revenue_range = get_val("revenue_range")

            # Location
            lead.location_city = get_val("location_city")
            lead.location_state = get_val("location_state")
            lead.location_country = get_val("location_country")

            # Set flags
            lead.has_contact = bool(lead.email or lead.linkedin_url or lead.phone)
            lead.needs_enrichment = not lead.has_contact or not lead.email

            # Only add if there's meaningful data
            if lead.company_name or lead.email or lead.company_domain:
                leads.append(lead)

        except Exception as e:
            print(f"Warning: Error processing row {idx}: {e}")
            continue

    return leads


def save_normalized(leads: List[Lead], output_path: str) -> str:
    """Save normalized leads to JSON file"""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Convert to dicts
    data = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "total_leads": len(leads),
        "leads_with_email": sum(1 for l in leads if l.email),
        "leads_with_company": sum(1 for l in leads if l.company_name),
        "needs_enrichment": sum(1 for l in leads if l.needs_enrichment),
        "leads": [asdict(l) for l in leads]
    }

    # Remove raw_data from output (too verbose)
    for lead in data["leads"]:
        lead.pop("raw_data", None)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

    return output_path


def ingest_file(file_path: str) -> List[Lead]:
    """Ingest leads from file based on extension"""
    file_path = os.path.abspath(file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    ext = os.path.splitext(file_path)[1].lower()

    if ext in [".xlsx", ".xls"]:
        return ingest_excel(file_path)
    elif ext == ".csv":
        return ingest_csv(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}. Use .xlsx, .xls, or .csv")


def main():
    parser = argparse.ArgumentParser(description="Ingest and normalize lead data")
    parser.add_argument("file", help="Input file path (Excel or CSV)")
    parser.add_argument("--output", "-o", help="Output JSON file path",
                        default=".tmp/leads/normalized.json")

    args = parser.parse_args()

    print(f"Ingesting leads from: {args.file}")

    leads = ingest_file(args.file)

    print(f"\nProcessed {len(leads)} leads:")
    print(f"  - With email: {sum(1 for l in leads if l.email)}")
    print(f"  - With company: {sum(1 for l in leads if l.company_name)}")
    print(f"  - Needs enrichment: {sum(1 for l in leads if l.needs_enrichment)}")

    output_path = save_normalized(leads, args.output)
    print(f"\nSaved to: {output_path}")


if __name__ == "__main__":
    main()
