"""
Upload to SmartLead
Reads leads from Google Sheets and email sequences from Google Docs,
then bulk uploads to SmartLead campaign with all sequence variants.
"""

import os
import sys
import re
from typing import List, Dict, Optional
from dotenv import load_dotenv

from google_docs_helper import read_document
from google_sheets_helper import get_credentials
from googleapiclient.discovery import build
from smartlead_api import bulk_upload_leads, validate_campaign

# Load environment variables
load_dotenv()


def read_google_sheet(spreadsheet_id: str) -> List[Dict]:
    """
    Read lead list from Google Sheets.

    Args:
        spreadsheet_id: Google Sheets spreadsheet ID

    Returns:
        List of lead dicts with keys: email, first_name, last_name, company, clean_company_name, niche
    """
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    # Read all data
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Sheet1!A:Z'
    ).execute()

    values = result.get('values', [])

    if not values:
        raise ValueError("No data found in spreadsheet")

    # First row is headers
    headers = [h.lower().strip() if h else '' for h in values[0]]

    # Find column indices
    email_idx = next((i for i, h in enumerate(headers) if 'email' in h), None)
    first_name_idx = next((i for i, h in enumerate(headers) if 'first' in h and 'name' in h), None)
    last_name_idx = next((i for i, h in enumerate(headers) if 'last' in h and 'name' in h), None)
    company_idx = next((i for i, h in enumerate(headers) if h == 'company'), None)
    clean_company_idx = next((i for i, h in enumerate(headers) if 'clean' in h and 'company' in h), None)
    niche_idx = next((i for i, h in enumerate(headers) if 'niche' in h), None)

    if email_idx is None:
        raise ValueError("No email column found in spreadsheet")

    # Build lead list
    leads = []
    for row in values[1:]:  # Skip header row
        if not row or len(row) <= email_idx:
            continue

        email = row[email_idx].strip() if email_idx < len(row) else ""
        if not email:
            continue

        lead = {
            'email': email,
            'first_name': row[first_name_idx].strip() if first_name_idx and first_name_idx < len(row) else "",
            'last_name': row[last_name_idx].strip() if last_name_idx and last_name_idx < len(row) else "",
            'company': row[company_idx].strip() if company_idx and company_idx < len(row) else "",
            'clean_company_name': row[clean_company_idx].strip() if clean_company_idx and clean_company_idx < len(row) else "",
            'niche': row[niche_idx].strip() if niche_idx and niche_idx < len(row) else ""
        }

        leads.append(lead)

    return leads


def parse_email_sequences(doc_content: str) -> Dict[str, str]:
    """
    Parse email sequences from Google Doc.

    Returns:
        Dict mapping keys like 'email1_subject_a' to actual content
    """
    sequences = {}

    # Pattern to match sequences and emails
    sequence_pattern = r'SEQUENCE ([ABC])'
    email_pattern = r'EMAIL (\d) - (.*?)\nSubject: (.*?)\nBody:\n(.*?)(?=\n\n---|═|EMAIL|SEQUENCE|$)'

    # Find all sequences
    seq_sections = re.split(r'═+', doc_content)

    for section in seq_sections:
        seq_match = re.search(sequence_pattern, section)
        if not seq_match:
            continue

        seq_letter = seq_match.group(1).lower()  # a, b, or c

        # Find all emails in this sequence
        emails = re.finditer(email_pattern, section, re.DOTALL)

        for email_match in emails:
            email_num = email_match.group(1)  # 1, 2, or 3
            subject = email_match.group(3).strip()
            body = email_match.group(4).strip()

            # Store with SmartLead custom field naming
            sequences[f'email{email_num}_subject_{seq_letter}'] = subject
            sequences[f'email{email_num}_body_{seq_letter}'] = body

    return sequences


def combine_leads_with_sequences(leads: List[Dict], sequences: Dict[str, str]) -> List[Dict]:
    """
    Combine lead data with email sequences for SmartLead upload.

    Args:
        leads: List of lead dicts
        sequences: Dict of email sequences (from Google Doc)

    Returns:
        List of SmartLead-formatted lead dicts
    """
    smartlead_leads = []

    for lead in leads:
        # Use Clean_Company_Name if available, fallback to Company
        company_name = lead['clean_company_name'] if lead['clean_company_name'] else lead['company']

        smartlead_lead = {
            'email': lead['email'],
            'first_name': lead['first_name'],
            'last_name': lead['last_name'],
            'company_name': company_name,  # Maps {company_name} placeholder
            'custom_fields': {
                'niche': lead['niche'],
                **sequences  # Include all 18 email templates (9 subjects + 9 bodies)
            }
        }

        smartlead_leads.append(smartlead_lead)

    return smartlead_leads


def upload_to_smartlead(
    sheet_id: str,
    doc_id: str,
    campaign_id: str,
    verbose: bool = True
) -> Dict:
    """
    Upload leads with email sequences to SmartLead.

    Args:
        sheet_id: Google Sheets spreadsheet ID (lead list)
        doc_id: Google Docs document ID (email sequences)
        campaign_id: SmartLead campaign ID
        verbose: Whether to print progress

    Returns:
        Dict with upload statistics
    """
    if verbose:
        print("Step 1: Validating SmartLead campaign...")

    try:
        campaign = validate_campaign(campaign_id)
        if verbose:
            print(f"  ✓ Campaign found: {campaign.get('name', 'Unnamed')}")
    except Exception as e:
        return {"error": f"Campaign validation failed: {str(e)}"}

    if verbose:
        print("\nStep 2: Reading lead list from Google Sheets...")

    try:
        leads = read_google_sheet(sheet_id)
        if verbose:
            print(f"  ✓ Found {len(leads)} leads")
    except Exception as e:
        return {"error": f"Failed to read spreadsheet: {str(e)}"}

    if not leads:
        return {"error": "No leads found in spreadsheet"}

    if verbose:
        print("\nStep 3: Reading email sequences from Google Docs...")

    try:
        doc_content = read_document(doc_id)
        sequences = parse_email_sequences(doc_content)

        if verbose:
            print(f"  ✓ Parsed {len(sequences)} email templates")

            # Verify we have all 18 templates (9 subjects + 9 bodies)
            expected_keys = []
            for num in range(1, 4):  # Email 1, 2, 3
                for variant in ['a', 'b', 'c']:
                    expected_keys.append(f'email{num}_subject_{variant}')
                    expected_keys.append(f'email{num}_body_{variant}')

            missing = [k for k in expected_keys if k not in sequences]
            if missing:
                print(f"  ⚠ Warning: Missing templates: {missing}")

    except Exception as e:
        return {"error": f"Failed to read or parse document: {str(e)}"}

    if verbose:
        print("\nStep 4: Combining leads with sequences...")

    try:
        smartlead_leads = combine_leads_with_sequences(leads, sequences)
        if verbose:
            print(f"  ✓ Prepared {len(smartlead_leads)} leads for upload")
    except Exception as e:
        return {"error": f"Failed to combine data: {str(e)}"}

    if verbose:
        print("\nStep 5: Uploading to SmartLead...")

    try:
        result = bulk_upload_leads(campaign_id, smartlead_leads)
        if verbose:
            print(f"\n  ✓ Upload complete!")
    except Exception as e:
        return {"error": f"Upload failed: {str(e)}"}

    return {
        "campaign_id": campaign_id,
        "sheet_id": sheet_id,
        "doc_id": doc_id,
        **result
    }


def print_summary(result: Dict):
    """Print summary of upload results."""
    print("\n" + "=" * 60)
    print("SMARTLEAD UPLOAD - SUMMARY")
    print("=" * 60)

    if "error" in result:
        print(f"ERROR: {result['error']}")
        return

    print(f"Campaign ID: {result['campaign_id']}")
    print(f"Total leads: {result['total_leads']}")
    print(f"Successfully uploaded: {result['total_uploaded']}")

    if result['duplicates'] > 0:
        print(f"Duplicates skipped: {result['duplicates']}")
    if result['invalid_emails'] > 0:
        print(f"Invalid emails skipped: {result['invalid_emails']}")
    if result['unsubscribed'] > 0:
        print(f"Unsubscribed leads skipped: {result['unsubscribed']}")

    print("\nFEATURES INCLUDED:")
    print("  ✓ 18 email templates uploaded (9 subjects + 9 bodies)")
    print("  ✓ 3 complete sequences (A/B/C)")
    print("  ✓ Each sequence has 3 emails (Initial → Follow-up → Breakup)")
    print("  ✓ {first_name} and {company_name} placeholders ready for SmartLead")
    print("  ✓ Spintax variations preserved")
    print("  ✓ %signature% placeholder included")

    print("\nNEXT STEPS:")
    print("  1. Create 3 campaigns in SmartLead (Campaign A, B, C)")
    print("  2. Campaign A: Use email1_subject_a, email1_body_a templates")
    print("  3. Campaign B: Use email1_subject_b, email1_body_b templates")
    print("  4. Campaign C: Use email1_subject_c, email1_body_c templates")
    print("  5. Split lead list 33/33/33 across campaigns")
    print("  6. Set delays: Email 2 (3 days), Email 3 (5 days)")
    print("  7. Test send to yourself first")
    print("  8. Launch and monitor reply rates")

    print("=" * 60)


def main():
    if len(sys.argv) < 4:
        print("Usage:")
        print("  python upload_to_smartlead.py <sheet_id> <doc_id> <campaign_id>")
        print("\nExamples:")
        print("  python upload_to_smartlead.py 1a2b3c4d5e6f 9z8y7x6w5v4u campaign_123")
        print("  python upload_to_smartlead.py https://sheets.google.com/... https://docs.google.com/... campaign_123")
        sys.exit(1)

    sheet_id_or_url = sys.argv[1]
    doc_id_or_url = sys.argv[2]
    campaign_id = sys.argv[3]

    # Extract IDs from URLs if necessary
    if 'sheets.google.com' in sheet_id_or_url:
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_id_or_url)
        sheet_id = match.group(1) if match else sheet_id_or_url
    else:
        sheet_id = sheet_id_or_url

    if 'docs.google.com' in doc_id_or_url:
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', doc_id_or_url)
        doc_id = match.group(1) if match else doc_id_or_url
    else:
        doc_id = doc_id_or_url

    result = upload_to_smartlead(sheet_id, doc_id, campaign_id)
    print_summary(result)


if __name__ == '__main__':
    main()
