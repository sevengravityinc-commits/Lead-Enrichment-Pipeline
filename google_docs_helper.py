"""
Google Docs Helper
Provides functions for creating, reading, and updating Google Docs.
"""

import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/drive.file'
]


def get_credentials():
    """Get or refresh Google API credentials."""
    creds = None
    token_path = os.getenv('GOOGLE_TOKEN_PATH', 'token.json')
    credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH', 'credentials.json')

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds


def create_document(title: str) -> dict:
    """
    Create a blank Google Doc.

    Args:
        title: Document title

    Returns:
        {
            'document_id': 'abc123',
            'url': 'https://docs.google.com/document/d/abc123/edit'
        }
    """
    creds = get_credentials()
    service = build('docs', 'v1', credentials=creds)

    doc = service.documents().create(body={'title': title}).execute()

    return {
        'document_id': doc['documentId'],
        'url': f"https://docs.google.com/document/d/{doc['documentId']}/edit"
    }


def update_document(document_id: str, content: str):
    """
    Replace document content with new text.

    Args:
        document_id: Google Docs document ID
        content: New text content to insert
    """
    creds = get_credentials()
    service = build('docs', 'v1', credentials=creds)

    # Get document to find end index
    doc = service.documents().get(documentId=document_id).execute()

    # Find the last index (end of document)
    end_index = doc['body']['content'][-1]['endIndex'] - 1 if doc['body']['content'] else 1

    requests = []

    # Delete existing content (if any)
    if end_index > 1:
        requests.append({
            'deleteContentRange': {
                'range': {
                    'startIndex': 1,
                    'endIndex': end_index
                }
            }
        })

    # Insert new content
    requests.append({
        'insertText': {
            'location': {'index': 1},
            'text': content
        }
    })

    # Execute batch update
    service.documents().batchUpdate(
        documentId=document_id,
        body={'requests': requests}
    ).execute()


def read_document(document_id: str) -> str:
    """
    Extract text from a Google Doc.

    Args:
        document_id: Google Docs document ID

    Returns:
        Full text content of the document
    """
    creds = get_credentials()
    service = build('docs', 'v1', credentials=creds)

    doc = service.documents().get(documentId=document_id).execute()

    text = []
    for element in doc.get('body', {}).get('content', []):
        if 'paragraph' in element:
            for text_run in element['paragraph'].get('elements', []):
                if 'textRun' in text_run:
                    text.append(text_run['textRun']['content'])

    return ''.join(text)


def share_document(document_id: str, email: str, role: str = 'writer'):
    """
    Share document with a user (optional, for future use).

    Args:
        document_id: Google Docs document ID
        email: Email address to share with
        role: Permission role ('reader', 'writer', 'commenter')
    """
    creds = get_credentials()
    drive_service = build('drive', 'v3', credentials=creds)

    permission = {
        'type': 'user',
        'role': role,
        'emailAddress': email
    }

    drive_service.permissions().create(
        fileId=document_id,
        body=permission,
        fields='id',
        supportsAllDrives=True
    ).execute()


if __name__ == '__main__':
    # Test creating and updating a document
    result = create_document('Test Email Sequences')
    print(f"Created document: {result['url']}")

    test_content = """
EMAIL SEQUENCES - Test Niche
Generated: 2026-01-29

═══════════════════════════════════════
SEQUENCE A (Problem-focused, direct)
═══════════════════════════════════════

EMAIL 1 - INITIAL OUTREACH
Subject: Test subject
Body:
Hey {first_name},

This is a test email sequence.

Mind if I share a quick video?
"""

    update_document(result['document_id'], test_content)
    print("Updated document with test content")

    # Read back the content
    read_content = read_document(result['document_id'])
    print(f"\nRead content:\n{read_content}")
