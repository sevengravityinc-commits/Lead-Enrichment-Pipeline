"""
Google Drive Helper
Provides functions for managing folders and files in Google Drive (including Shared Drives).
"""

import os
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
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


def find_shared_drive(drive_name: str) -> str:
    """
    Find a shared drive by name.

    Args:
        drive_name: Name of the shared drive (e.g., 'Seven Gravity')

    Returns:
        Drive ID if found, None otherwise
    """
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    response = service.drives().list(
        pageSize=100,
        fields='drives(id, name)'
    ).execute()

    for drive in response.get('drives', []):
        if drive['name'] == drive_name:
            return drive['id']

    return None


def find_folder_in_drive(drive_id: str, folder_name: str, parent_folder_id: str = None) -> str:
    """
    Find a folder by name within a shared drive or parent folder.

    Args:
        drive_id: Shared drive ID
        folder_name: Name of folder to find (e.g., 'Email Campaigns')
        parent_folder_id: Optional parent folder to search within

    Returns:
        Folder ID if found, None otherwise
    """
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    # Build query
    query_parts = [
        f"mimeType='application/vnd.google-apps.folder'",
        f"name='{folder_name}'",
        f"trashed=false"
    ]

    if parent_folder_id:
        query_parts.append(f"'{parent_folder_id}' in parents")

    query = ' and '.join(query_parts)

    response = service.files().list(
        q=query,
        driveId=drive_id,
        corpora='drive',
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields='files(id, name)',
        pageSize=100
    ).execute()

    files = response.get('files', [])
    if files:
        return files[0]['id']

    return None


def create_folder(folder_name: str, parent_folder_id: str, drive_id: str) -> str:
    """
    Create a folder in a shared drive.

    Args:
        folder_name: Name of the new folder
        parent_folder_id: Parent folder ID
        drive_id: Shared drive ID

    Returns:
        New folder ID
    """
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id],
        'driveId': drive_id
    }

    folder = service.files().create(
        body=file_metadata,
        supportsAllDrives=True,
        fields='id'
    ).execute()

    return folder.get('id')


def move_file_to_folder(file_id: str, folder_id: str):
    """
    Move a file to a specific folder.

    Args:
        file_id: ID of file to move
        folder_id: Destination folder ID
    """
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    # Get current parents
    file = service.files().get(
        fileId=file_id,
        fields='parents',
        supportsAllDrives=True
    ).execute()

    previous_parents = ','.join(file.get('parents', []))

    # Move file
    service.files().update(
        fileId=file_id,
        addParents=folder_id,
        removeParents=previous_parents,
        supportsAllDrives=True,
        fields='id, parents'
    ).execute()


def get_or_create_campaign_folders(niche: str, date: str = None) -> dict:
    """
    Get or create folder structure: Email Campaigns/{Niche}/{Date}/

    Args:
        niche: Campaign niche (e.g., 'Marketing')
        date: Date string (e.g., '2026-01-29'). If None, uses today's date.

    Returns:
        {
            'drive_id': 'xxx',
            'campaigns_folder_id': 'xxx',
            'niche_folder_id': 'xxx',
            'date_folder_id': 'xxx',
            'date_folder_url': 'https://drive.google.com/...'
        }
    """
    # Use today's date if not provided
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')

    # Step 1: Find Seven Gravity shared drive
    drive_id = find_shared_drive('Seven Gravity')
    if not drive_id:
        raise ValueError("Seven Gravity shared drive not found")

    # Step 2: Find Email Campaigns folder
    campaigns_folder_id = find_folder_in_drive(drive_id, 'Email Campaigns')
    if not campaigns_folder_id:
        raise ValueError("Email Campaigns folder not found in Seven Gravity drive")

    # Step 3: Check if niche folder exists, create if not
    niche_folder_id = find_folder_in_drive(drive_id, niche, campaigns_folder_id)
    if not niche_folder_id:
        niche_folder_id = create_folder(niche, campaigns_folder_id, drive_id)
        print(f"Created niche folder: {niche}")
    else:
        print(f"Found existing niche folder: {niche}")

    # Step 4: Create date subfolder (always new)
    date_folder_id = create_folder(date, niche_folder_id, drive_id)
    print(f"Created date folder: {date}")

    # Build folder URL
    date_folder_url = f"https://drive.google.com/drive/folders/{date_folder_id}"

    return {
        'drive_id': drive_id,
        'campaigns_folder_id': campaigns_folder_id,
        'niche_folder_id': niche_folder_id,
        'date_folder_id': date_folder_id,
        'date_folder_url': date_folder_url
    }


def create_shared_drive(drive_name: str) -> dict:
    """
    Create a new shared drive or return existing one if it already exists.

    Args:
        drive_name: Name of the shared drive to create (e.g., 'Acme Corp')

    Returns:
        {
            'drive_id': 'xxx',
            'drive_url': 'https://drive.google.com/drive/folders/xxx',
            'created': True/False (False if already existed)
        }
    """
    import uuid

    # First check if drive already exists
    existing_drive_id = find_shared_drive(drive_name)
    if existing_drive_id:
        print(f"Found existing shared drive: {drive_name}")
        return {
            'drive_id': existing_drive_id,
            'drive_url': f"https://drive.google.com/drive/folders/{existing_drive_id}",
            'created': False
        }

    # Create new shared drive
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    # requestId must be unique for idempotency
    request_id = str(uuid.uuid4())

    drive_metadata = {
        'name': drive_name
    }

    drive = service.drives().create(
        requestId=request_id,
        body=drive_metadata,
        fields='id, name'
    ).execute()

    drive_id = drive.get('id')
    print(f"Created new shared drive: {drive_name}")

    return {
        'drive_id': drive_id,
        'drive_url': f"https://drive.google.com/drive/folders/{drive_id}",
        'created': True
    }


def add_drive_member(drive_id: str, email: str, role: str = 'reader') -> bool:
    """
    Add a user to a shared drive with specified role.

    Args:
        drive_id: The shared drive ID
        email: Email address of user to add
        role: Permission role - 'reader' (viewer), 'commenter', 'writer' (editor), 'organizer'

    Returns:
        True if successful, False otherwise
    """
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    permission = {
        'type': 'user',
        'role': role,
        'emailAddress': email
    }

    try:
        service.permissions().create(
            fileId=drive_id,
            body=permission,
            supportsAllDrives=True,
            sendNotificationEmail=True
        ).execute()
        print(f"Added {email} as {role} to drive")
        return True
    except Exception as e:
        print(f"Failed to add {email} to drive: {e}")
        return False


def create_folders_in_shared_drive(drive_id: str, folder_names: list) -> dict:
    """
    Create multiple folders at the root of a shared drive.

    Args:
        drive_id: The shared drive ID
        folder_names: List of folder names to create (e.g., ['Deliverables', 'Resources', 'Meeting Notes'])

    Returns:
        {
            'folder_name': {
                'id': 'xxx',
                'url': 'https://drive.google.com/drive/folders/xxx'
            },
            ...
        }
    """
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    results = {}

    for folder_name in folder_names:
        # Check if folder already exists at root of drive
        existing_id = find_folder_in_drive(drive_id, folder_name, drive_id)
        if existing_id:
            print(f"Found existing folder: {folder_name}")
            results[folder_name] = {
                'id': existing_id,
                'url': f"https://drive.google.com/drive/folders/{existing_id}"
            }
            continue

        # Create new folder
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [drive_id]
        }

        folder = service.files().create(
            body=file_metadata,
            supportsAllDrives=True,
            fields='id'
        ).execute()

        folder_id = folder.get('id')
        print(f"Created folder: {folder_name}")

        results[folder_name] = {
            'id': folder_id,
            'url': f"https://drive.google.com/drive/folders/{folder_id}"
        }

    return results


if __name__ == '__main__':
    # Test folder creation
    result = get_or_create_campaign_folders('Test Niche', '2026-01-29')
    print(f"\nFolder structure created:")
    print(f"  Drive ID: {result['drive_id']}")
    print(f"  Date folder URL: {result['date_folder_url']}")
