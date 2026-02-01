"""
Create a task in ClickUp from Smartlead webhook data.
This script receives lead data from Smartlead and creates a corresponding task in ClickUp.
"""

import os
import requests
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CLICKUP_API_KEY = os.getenv('CLICKUP_API_KEY')
CLICKUP_LIST_ID = os.getenv('CLICKUP_LIST_ID')

def create_clickup_task(lead_data):
    """
    Create a task in ClickUp with lead information.

    Args:
        lead_data (dict): Dictionary containing lead information from Smartlead
            - lead_name: Name of the lead
            - email: Email address
            - company: Company name
            - campaign_name: Name of the campaign
            - lead_status: Status (interested/meeting request)
            - last_contacted_date: ISO date string

    Returns:
        dict: Response from ClickUp API with task details
    """

    if not CLICKUP_API_KEY:
        raise ValueError("CLICKUP_API_KEY not found in environment variables")

    if not CLICKUP_LIST_ID:
        raise ValueError("CLICKUP_LIST_ID not found in environment variables")

    # Extract lead information
    lead_name = lead_data.get('lead_name', 'Unknown Lead')
    email = lead_data.get('email', '')
    company = lead_data.get('company', '')
    campaign_name = lead_data.get('campaign_name', '')
    lead_status = lead_data.get('lead_status', '')
    last_contacted = lead_data.get('last_contacted_date', '')

    # Build task name
    task_name = f"{lead_name}"
    if company:
        task_name += f" - {company}"

    # Build task description with all lead details
    description_parts = []
    if email:
        description_parts.append(f"**Email:** {email}")
    if company:
        description_parts.append(f"**Company:** {company}")
    if campaign_name:
        description_parts.append(f"**Campaign:** {campaign_name}")
    if lead_status:
        description_parts.append(f"**Status:** {lead_status}")
    if last_contacted:
        description_parts.append(f"**Last Contacted:** {last_contacted}")

    description = "\n".join(description_parts)

    # ClickUp API endpoint
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"

    headers = {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json"
    }

    # Task payload
    payload = {
        "name": task_name,
        "description": description,
        "status": "new lead",  # Set to new lead status as requested
        "priority": 3,  # Normal priority
        "notify_all": True
    }

    # Make the API request
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        task_data = response.json()
        print(f"[SUCCESS] Task created successfully in ClickUp: {task_name}")
        print(f"  Task ID: {task_data['id']}")
        print(f"  URL: {task_data['url']}")
        return task_data
    else:
        error_msg = f"Failed to create task. Status: {response.status_code}, Response: {response.text}"
        print(f"[ERROR] {error_msg}", file=sys.stderr)
        raise Exception(error_msg)

def main():
    """
    Main function for CLI usage.
    Expects lead data as JSON string or file path.
    """
    if len(sys.argv) < 2:
        print("Usage: python create_clickup_task.py '<json_data>' or python create_clickup_task.py <json_file>")
        print("\nExample:")
        print('python create_clickup_task.py \'{"lead_name": "John Doe", "email": "john@example.com", "company": "Acme Inc", "campaign_name": "Q1 Outreach", "lead_status": "interested", "last_contacted_date": "2026-01-24"}\'')
        sys.exit(1)

    input_arg = sys.argv[1]

    # Check if it's a file path
    if os.path.isfile(input_arg):
        with open(input_arg, 'r') as f:
            lead_data = json.load(f)
    else:
        # Parse as JSON string
        lead_data = json.loads(input_arg)

    result = create_clickup_task(lead_data)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
