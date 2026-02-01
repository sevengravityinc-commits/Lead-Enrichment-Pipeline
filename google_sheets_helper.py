"""
Google Sheets Helper
Provides functions for creating and updating Google Sheets.
"""

import os
import json
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/documents',
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


def create_spreadsheet(title: str) -> dict:
    """Create a new Google Spreadsheet."""
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    spreadsheet = {
        'properties': {'title': title}
    }

    result = service.spreadsheets().create(body=spreadsheet).execute()
    return {
        'spreadsheet_id': result['spreadsheetId'],
        'url': result['spreadsheetUrl']
    }


def add_sheet(spreadsheet_id: str, sheet_name: str) -> int:
    """Add a new sheet/tab to an existing spreadsheet."""
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    request = {
        'requests': [{
            'addSheet': {
                'properties': {'title': sheet_name}
            }
        }]
    }

    result = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request
    ).execute()

    return result['replies'][0]['addSheet']['properties']['sheetId']


def write_to_sheet(spreadsheet_id: str, sheet_name: str, data: list, start_cell: str = 'A1'):
    """Write data to a specific sheet."""
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    range_name = f"'{sheet_name}'!{start_cell}"

    body = {
        'values': data
    }

    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()

    return result


def append_to_sheet(spreadsheet_id: str, sheet_name: str, data: list):
    """Append rows to a sheet."""
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    range_name = f"'{sheet_name}'!A:Z"

    body = {
        'values': data
    }

    result = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        insertDataOption='INSERT_ROWS',
        body=body
    ).execute()

    return result


def format_header_row(spreadsheet_id: str, sheet_id: int):
    """Format the header row (bold, background color)."""
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    requests = [
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.6},
                        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat)'
            }
        },
        {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {'frozenRowCount': 1}
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        }
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests}
    ).execute()


def delete_default_sheet(spreadsheet_id: str):
    """Delete the default 'Sheet1' if it exists."""
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    # Get spreadsheet metadata
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == 'Sheet1':
            request = {
                'requests': [{
                    'deleteSheet': {'sheetId': sheet['properties']['sheetId']}
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request
            ).execute()
            break


def setup_categorization_sheet(title: str) -> dict:
    """Create a spreadsheet with tabs for 3PL categorization."""
    # Create the spreadsheet
    result = create_spreadsheet(title)
    spreadsheet_id = result['spreadsheet_id']

    # Define tabs and headers
    tabs = {
        '3PL_FULFILLMENT': ['Company Name', 'Website', 'Contact Name', 'Contact Email', 'Phone', 'LinkedIn', 'City', 'State', 'Confidence', 'Reasoning', 'Keywords Used'],
        'PORT_TRANSIT': ['Company Name', 'Website', 'Contact Name', 'Contact Email', 'Phone', 'LinkedIn', 'City', 'State', 'Confidence', 'Reasoning', 'Keywords Used'],
        'SPECIALIZED_STORAGE': ['Company Name', 'Website', 'Contact Name', 'Contact Email', 'Phone', 'LinkedIn', 'City', 'State', 'Confidence', 'Reasoning', 'Keywords Used'],
        'NEEDS_REVIEW': ['Company Name', 'Website', 'Contact Name', 'Contact Email', 'Phone', 'LinkedIn', 'City', 'State', 'Confidence', 'Reasoning', 'Keywords Used']
    }

    sheet_ids = {}

    for tab_name, headers in tabs.items():
        sheet_id = add_sheet(spreadsheet_id, tab_name)
        sheet_ids[tab_name] = sheet_id
        write_to_sheet(spreadsheet_id, tab_name, [headers])
        format_header_row(spreadsheet_id, sheet_id)

    # Delete the default Sheet1
    delete_default_sheet(spreadsheet_id)

    return {
        'spreadsheet_id': spreadsheet_id,
        'url': result['url'],
        'sheet_ids': sheet_ids
    }


if __name__ == '__main__':
    # Test creating a categorization sheet
    result = setup_categorization_sheet('3PL Lead Categorization - Test')
    print(f"Created spreadsheet: {result['url']}")
