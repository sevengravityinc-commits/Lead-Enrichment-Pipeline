"""
Gmail Helper
Provides functions for creating and managing Gmail drafts.
"""

import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/gmail.compose',
    'https://www.googleapis.com/auth/gmail.modify'
]

# Use separate token for Gmail to avoid scope conflicts with Drive/Sheets
GMAIL_TOKEN_PATH = 'gmail_token.json'


def get_credentials():
    """Get or refresh Google API credentials for Gmail."""
    creds = None
    token_path = os.getenv('GMAIL_TOKEN_PATH', GMAIL_TOKEN_PATH)
    credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH', 'credentials.json')

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print(f"\nGmail authorization required. A browser window will open...")
            print(f"(Token will be saved to: {token_path})\n")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds


def create_message(to: str, subject: str, body_html: str, from_email: str = None, cc: str = None) -> dict:
    """
    Create an email message in MIME format.

    Args:
        to: Recipient email address
        subject: Email subject
        body_html: HTML body content
        from_email: Sender email (optional, uses authenticated user if not provided)
        cc: CC email address (optional)

    Returns:
        Dictionary with 'raw' key containing base64 encoded message
    """
    message = MIMEMultipart('alternative')
    message['to'] = to
    message['subject'] = subject

    if from_email:
        message['from'] = from_email

    if cc:
        message['cc'] = cc

    # Create HTML part
    html_part = MIMEText(body_html, 'html')
    message.attach(html_part)

    # Encode the message
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')

    return {'raw': raw_message}


def create_draft(to: str, subject: str, body_html: str, from_email: str = None, cc: str = None) -> dict:
    """
    Create a draft email in Gmail.

    Args:
        to: Recipient email address
        subject: Email subject
        body_html: HTML body content
        from_email: Sender email (optional)
        cc: CC email address (optional)

    Returns:
        Dictionary with draft details including 'id' and 'message'
    """
    creds = get_credentials()
    service = build('gmail', 'v1', credentials=creds)

    message = create_message(to, subject, body_html, from_email, cc)

    draft = service.users().drafts().create(
        userId='me',
        body={'message': message}
    ).execute()

    print(f"Draft created with ID: {draft['id']}")

    return draft


def get_draft_link(draft_id: str) -> str:
    """
    Get a link to open the draft in Gmail.

    Args:
        draft_id: The draft ID returned from create_draft

    Returns:
        URL to open the draft in Gmail web interface
    """
    return f"https://mail.google.com/mail/u/0/#drafts?compose={draft_id}"


if __name__ == '__main__':
    # Test draft creation
    test_draft = create_draft(
        to="test@example.com",
        subject="Test Draft",
        body_html="<h1>Test</h1><p>This is a test draft.</p>"
    )
    print(f"Draft link: {get_draft_link(test_draft['id'])}")
