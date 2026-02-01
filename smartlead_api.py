"""
SmartLead API Wrapper
Provides functions for interacting with the SmartLead API.
Handles bulk lead upload to campaigns.
"""

import os
import requests
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SMARTLEAD_BASE_URL = "https://server.smartlead.ai/api/v1"


def get_api_key() -> str:
    """Get SmartLead API key from environment."""
    api_key = os.getenv('SMARTLEAD_API_KEY')
    if not api_key:
        raise ValueError("SMARTLEAD_API_KEY not found in .env file")
    return api_key


def validate_campaign(campaign_id: str) -> Dict:
    """
    Validate that a campaign exists.

    Args:
        campaign_id: SmartLead campaign ID

    Returns:
        Campaign info dict if exists

    Raises:
        ValueError: If campaign doesn't exist or API error
    """
    api_key = get_api_key()
    url = f"{SMARTLEAD_BASE_URL}/campaigns/{campaign_id}"

    response = requests.get(
        url,
        params={'api_key': api_key}
    )

    if response.status_code == 404:
        raise ValueError(f"Campaign {campaign_id} not found")

    response.raise_for_status()
    return response.json()


def bulk_upload_leads(campaign_id: str, leads: List[Dict], batch_size: int = 350) -> Dict:
    """
    Upload leads to SmartLead campaign in batches.

    Args:
        campaign_id: SmartLead campaign ID
        leads: List of lead dicts with email, first_name, etc.
        batch_size: Max 350 per batch (API limit)

    Returns:
        {
            'total_uploaded': int,
            'total_leads': int,
            'duplicates': int,
            'invalid_emails': int,
            'unsubscribed': int,
            'batches': [list of batch results]
        }
    """
    api_key = get_api_key()
    url = f"{SMARTLEAD_BASE_URL}/campaigns/{campaign_id}/leads"

    results = {
        'total_uploaded': 0,
        'total_leads': len(leads),
        'duplicates': 0,
        'invalid_emails': 0,
        'unsubscribed': 0,
        'batches': []
    }

    # Process in batches of 350 (API limit)
    for i in range(0, len(leads), batch_size):
        batch = leads[i:i + batch_size]
        batch_num = (i // batch_size) + 1

        print(f"Uploading batch {batch_num} ({len(batch)} leads)...")

        payload = {
            "lead_list": batch,
            "ignore_global_block_list": False,
            "ignore_unsubscribe_list": False,
            "ignore_duplicate_leads_in_other_campaign": True
        }

        try:
            response = requests.post(
                url,
                params={'api_key': api_key},
                json=payload
            )
            response.raise_for_status()

            result = response.json()

            # Aggregate statistics
            results['total_uploaded'] += result.get('upload_count', 0)
            results['duplicates'] += result.get('duplicate_count', 0)
            results['invalid_emails'] += result.get('invalid_email_count', 0)
            results['unsubscribed'] += result.get('unsubscribed_leads', 0)

            results['batches'].append({
                'batch_num': batch_num,
                'size': len(batch),
                **result
            })

            print(f"  ✓ Batch {batch_num}: {result.get('upload_count', 0)} uploaded")

        except Exception as e:
            print(f"  ✗ Batch {batch_num} failed: {str(e)}")
            results['batches'].append({
                'batch_num': batch_num,
                'size': len(batch),
                'error': str(e)
            })

    return results


def get_campaigns() -> List[Dict]:
    """
    Fetch list of all campaigns.

    Returns:
        List of campaign dicts
    """
    api_key = get_api_key()
    url = f"{SMARTLEAD_BASE_URL}/campaigns"

    response = requests.get(
        url,
        params={'api_key': api_key}
    )
    response.raise_for_status()

    return response.json()


if __name__ == '__main__':
    # Test API connection
    print("Testing SmartLead API connection...")

    try:
        campaigns = get_campaigns()
        print(f"✓ Connected successfully")
        print(f"Found {len(campaigns)} campaigns")

        if campaigns:
            print("\nFirst 5 campaigns:")
            for campaign in campaigns[:5]:
                print(f"  - {campaign.get('name', 'Unnamed')} (ID: {campaign.get('id')})")

    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
