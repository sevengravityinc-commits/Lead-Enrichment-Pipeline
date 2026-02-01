"""
Upload 3PL Classification Results to Google Sheets
"""

import csv
import os
from pathlib import Path
from datetime import datetime
from google_sheets_helper import (
    get_credentials, create_spreadsheet, add_sheet,
    write_to_sheet, format_header_row, delete_default_sheet
)
from dotenv import load_dotenv

load_dotenv()

def read_csv_file(filepath: str) -> list:
    """Read a CSV file and return list of rows."""
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)
    return rows


def upload_classification_results(classified_dir: str, sheet_title: str = None) -> dict:
    """Upload classification results to Google Sheets."""

    if sheet_title is None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        sheet_title = f"3PL Lead Categorization - {timestamp}"

    classified_path = Path(classified_dir)

    # Define tabs and their source files
    tabs = {
        '3PL_FULFILLMENT': classified_path / '3pl_fulfillment.csv',
        'PORT_TRANSIT': classified_path / 'port_transit.csv',
        'SPECIALIZED_STORAGE': classified_path / 'specialized_storage.csv',
        'NEEDS_REVIEW': classified_path / 'needs_review.csv'
    }

    # Create spreadsheet
    print(f"Creating spreadsheet: {sheet_title}")
    creds = get_credentials()

    from googleapiclient.discovery import build
    service = build('sheets', 'v4', credentials=creds)

    spreadsheet = {
        'properties': {'title': sheet_title}
    }
    result = service.spreadsheets().create(body=spreadsheet).execute()
    spreadsheet_id = result['spreadsheetId']
    spreadsheet_url = result['spreadsheetUrl']

    print(f"Spreadsheet created: {spreadsheet_url}")

    sheet_ids = {}
    total_rows = 0

    for tab_name, filepath in tabs.items():
        if not filepath.exists():
            print(f"Skipping {tab_name} - file not found")
            continue

        # Read data
        data = read_csv_file(filepath)
        if not data:
            print(f"Skipping {tab_name} - no data")
            continue

        # Add sheet
        request = {
            'requests': [{
                'addSheet': {
                    'properties': {'title': tab_name}
                }
            }]
        }
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request
        ).execute()
        sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
        sheet_ids[tab_name] = sheet_id

        # Write data
        range_name = f"'{tab_name}'!A1"
        body = {'values': data}
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

        # Format header
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
            },
            {
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 11
                    }
                }
            }
        ]
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()

        row_count = len(data) - 1  # Exclude header
        total_rows += row_count
        print(f"  {tab_name}: {row_count} leads uploaded")

    # Delete default Sheet1
    try:
        spreadsheet_meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet_meta['sheets']:
            if sheet['properties']['title'] == 'Sheet1':
                delete_request = {
                    'requests': [{
                        'deleteSheet': {'sheetId': sheet['properties']['sheetId']}
                    }]
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=delete_request
                ).execute()
                break
    except Exception as e:
        pass  # Ignore if Sheet1 doesn't exist

    print(f"\nTotal: {total_rows} leads uploaded to Google Sheets")
    print(f"URL: {spreadsheet_url}")

    return {
        'spreadsheet_id': spreadsheet_id,
        'url': spreadsheet_url,
        'sheet_ids': sheet_ids,
        'total_rows': total_rows
    }


if __name__ == '__main__':
    import sys

    classified_dir = sys.argv[1] if len(sys.argv) > 1 else '.tmp/classified'
    title = sys.argv[2] if len(sys.argv) > 2 else None

    result = upload_classification_results(classified_dir, title)
    print(f"\nDone! Sheet URL: {result['url']}")
