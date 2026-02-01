#!/usr/bin/env python3
"""
Morph Onboarding Script
Creates a shared drive for a new client and drafts an onboarding email.

Usage: python morph_onboarding.py "Company Name" "Contact First Name" "client@email.com"
"""

import os
import sys
import argparse
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

# Import helpers
from google_drive_helper import create_shared_drive, add_drive_member, create_folders_in_shared_drive
from gmail_helper import create_draft, get_draft_link

load_dotenv()

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
STANDARD_FOLDERS = ['Deliverables', 'Resources', 'Meeting Notes']


def retry_operation(operation, operation_name, *args, **kwargs):
    """
    Retry an operation up to MAX_RETRIES times.

    Args:
        operation: Function to call
        operation_name: Name for logging
        *args, **kwargs: Arguments to pass to operation

    Returns:
        Result of operation

    Raises:
        Exception if all retries fail
    """
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                print(f"  Attempt {attempt}/{MAX_RETRIES} failed for {operation_name}: {e}")
                print(f"  Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"  All {MAX_RETRIES} attempts failed for {operation_name}")

    raise last_error


def validate_email_domain(company_name: str, email: str) -> bool:
    """
    Check if email domain reasonably matches company name.
    Returns True if it seems to match, False if suspicious.
    """
    # Extract domain from email
    domain = email.split('@')[-1].lower()
    domain_name = domain.split('.')[0]

    # Normalize company name
    company_lower = company_name.lower()
    company_words = company_lower.replace('-', ' ').replace('_', ' ').split()

    # Check if any word from company name appears in domain
    for word in company_words:
        if len(word) > 2 and word in domain_name:
            return True

    # Check if domain name appears in company name
    if len(domain_name) > 2 and domain_name in company_lower:
        return True

    return False


def load_email_template():
    """Load the morph welcome email template."""
    template_path = Path(__file__).parent / 'templates' / 'morph_welcome_email.html'

    if not template_path.exists():
        raise FileNotFoundError(f"Email template not found: {template_path}")

    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()


def render_template(template: str, **variables) -> str:
    """Replace template placeholders with actual values."""
    rendered = template
    for key, value in variables.items():
        rendered = rendered.replace(f"{{{key}}}", str(value))
    return rendered


def send_slack_notification(message: str) -> bool:
    """Send a notification to Slack."""
    slack_url = os.getenv('SLACK_WEBHOOK_URL')
    if not slack_url:
        print("  No SLACK_WEBHOOK_URL configured, skipping notification")
        return False

    try:
        response = requests.post(slack_url, json={"text": message}, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"  Failed to send Slack notification: {e}")
        return False


def morph_onboard(company_name: str, contact_name: str, client_email: str) -> dict:
    """
    Main onboarding function.

    Args:
        company_name: Client's company name (used for shared drive name)
        contact_name: Client contact's first name
        client_email: Client's email address

    Returns:
        {
            'success': True/False,
            'drive_url': 'https://...',
            'resources_folder_url': 'https://...',
            'draft_link': 'https://...',
            'message': 'Status message'
        }
    """
    print(f"\n{'='*60}")
    print(f"MORPH ONBOARDING: {company_name}")
    print(f"{'='*60}")
    print(f"Contact: {contact_name}")
    print(f"Email: {client_email}")
    print(f"{'='*60}\n")

    # Step 1: Validate email domain
    print("Step 1: Validating email domain...")
    if not validate_email_domain(company_name, client_email):
        print(f"  WARNING: Email domain '{client_email.split('@')[-1]}' doesn't appear to match company '{company_name}'")
        print("  Proceeding anyway...")
    else:
        print("  Email domain looks valid")

    # Step 2: Create/find shared drive
    print("\nStep 2: Creating shared drive...")
    try:
        drive_result = retry_operation(
            create_shared_drive,
            "create shared drive",
            company_name
        )
        drive_id = drive_result['drive_id']
        drive_url = drive_result['drive_url']
        if drive_result['created']:
            print(f"  Created new shared drive: {company_name}")
        else:
            print(f"  Using existing shared drive: {company_name}")
        print(f"  Drive URL: {drive_url}")
    except Exception as e:
        return {
            'success': False,
            'message': f"Failed to create shared drive: {e}"
        }

    # Step 3: Add client as viewer
    print("\nStep 3: Adding client as viewer...")
    try:
        retry_operation(
            add_drive_member,
            "add drive member",
            drive_id, client_email, 'reader'
        )
        print(f"  Added {client_email} as viewer")
    except Exception as e:
        print(f"  WARNING: Failed to add client as viewer: {e}")
        print("  Continuing with other steps...")

    # Step 4: Create standard folders
    print("\nStep 4: Creating standard folders...")
    try:
        folders_result = retry_operation(
            create_folders_in_shared_drive,
            "create folders",
            drive_id, STANDARD_FOLDERS
        )
        for folder_name, folder_info in folders_result.items():
            print(f"  {folder_name}: {folder_info['url']}")

        # Get Resources folder URL for the email
        resources_folder_url = folders_result.get('Resources', {}).get('url', drive_url)
    except Exception as e:
        print(f"  WARNING: Failed to create folders: {e}")
        resources_folder_url = drive_url
        print("  Using drive root URL instead")

    # Step 5: Load and render email template
    print("\nStep 5: Preparing email...")
    tidycal_link = os.getenv('TIDYCAL_ONBOARDING_LINK', 'https://tidycal.com/m7vvnqr/onboarding-call')
    cc_email = os.getenv('ONBOARDING_CC_EMAIL', 'jennifer@sevengravity.com')

    try:
        template = load_email_template()
        email_body = render_template(
            template,
            contact_name=contact_name,
            company_name=company_name,
            shared_drive_link=resources_folder_url,
            tidycal_link=tidycal_link
        )
        print("  Email template loaded and rendered")
    except Exception as e:
        return {
            'success': False,
            'drive_url': drive_url,
            'resources_folder_url': resources_folder_url,
            'message': f"Failed to load email template: {e}"
        }

    # Step 6: Create Gmail draft
    print("\nStep 6: Creating Gmail draft...")
    subject = "Welcome to Seven Gravity - Onboarding"

    try:
        draft_result = retry_operation(
            create_draft,
            "create Gmail draft",
            client_email, subject, email_body, None, cc_email
        )
        draft_id = draft_result['id']
        draft_link = get_draft_link(draft_id)
        print(f"  Draft created successfully")
        print(f"  Draft link: {draft_link}")
    except Exception as e:
        return {
            'success': False,
            'drive_url': drive_url,
            'resources_folder_url': resources_folder_url,
            'message': f"Failed to create Gmail draft: {e}"
        }

    # Step 7: Send Slack notification
    print("\nStep 7: Sending Slack notification...")
    slack_message = f"""
:white_check_mark: *Onboarding Draft Ready*

*Client:* {company_name}
*Contact:* {contact_name} ({client_email})
*Drive:* {drive_url}
*Resources:* {resources_folder_url}
*Draft:* {draft_link}
""".strip()

    if send_slack_notification(slack_message):
        print("  Slack notification sent")
    else:
        print("  Slack notification skipped or failed")

    # Success!
    print(f"\n{'='*60}")
    print("ONBOARDING COMPLETE")
    print(f"{'='*60}")
    print(f"Company: {company_name}")
    print(f"Contact: {contact_name} ({client_email})")
    print(f"Shared Drive: {drive_url}")
    print(f"Resources Folder: {resources_folder_url}")
    print(f"Email Draft: {draft_link}")
    print(f"{'='*60}\n")

    return {
        'success': True,
        'drive_url': drive_url,
        'resources_folder_url': resources_folder_url,
        'draft_link': draft_link,
        'message': f"Onboarding draft ready for {company_name} ({contact_name})"
    }


def main():
    parser = argparse.ArgumentParser(
        description='Morph Onboarding - Create shared drive and draft onboarding email for new client'
    )
    parser.add_argument('company_name', help='Client company name')
    parser.add_argument('contact_name', help='Client contact first name')
    parser.add_argument('client_email', help='Client email address')

    args = parser.parse_args()

    result = morph_onboard(
        args.company_name,
        args.contact_name,
        args.client_email
    )

    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
