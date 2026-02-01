"""
Local webhook server for Smartlead to ClickUp integration.
Run this locally as an alternative to Modal deployment.

Usage:
  python execution/local_webhook_server.py

Then use a service like ngrok to expose it to the internet:
  ngrok http 5000

Set the ngrok URL as your Smartlead webhook.
"""

from flask import Flask, request, jsonify
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)

CLICKUP_API_KEY = os.getenv('CLICKUP_API_KEY')
CLICKUP_LIST_ID = os.getenv('CLICKUP_LIST_ID')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        "success": True,
        "message": "Webhook server is running",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/smartlead-intake', methods=['POST'])
def smartlead_intake():
    """
    Handle Smartlead webhook for interested/meeting request leads.

    Expected JSON payload:
    {
        "lead_name": "John Doe",
        "email": "john@example.com",
        "company": "Acme Inc",
        "campaign_name": "Q1 Outreach",
        "lead_status": "interested",
        "last_contacted_date": "2026-01-24"
    }
    """
    try:
        data = request.get_json()

        # Log the entire payload for debugging
        print("="*60)
        print("[WEBHOOK] Received payload from Smartlead:")
        print(json.dumps(data, indent=2))
        print("="*60)

        # Log request
        log_message = f"[WEBHOOK] Received Smartlead webhook"
        print(log_message)

        # Send Slack notification if configured
        if SLACK_WEBHOOK_URL:
            try:
                requests.post(SLACK_WEBHOOK_URL, json={"text": log_message})
            except:
                pass

        # Check API credentials
        if not CLICKUP_API_KEY or not CLICKUP_LIST_ID:
            return jsonify({
                "success": False,
                "error": "Missing ClickUp API credentials in environment"
            }), 500

        # Flexible field extraction - try multiple possible field names
        # Smartlead might use different naming conventions
        lead_status = (
            data.get("lead_status") or
            data.get("status") or
            data.get("lead", {}).get("status") or
            ""
        ).lower()

        # Only process if status is interested or meeting request
        # If no status field, we'll process anyway (you can change this)
        if lead_status and lead_status not in ["interested", "meeting request"]:
            print(f"[INFO] Skipping lead - status '{lead_status}' not in trigger list")
            return jsonify({
                "success": False,
                "message": f"Status '{lead_status}' does not trigger ClickUp creation."
            }), 200

        # Prepare lead data - try multiple field variations
        lead_name = (
            data.get("lead_name") or
            data.get("name") or
            data.get("first_name", "") + " " + data.get("last_name", "") or
            data.get("lead", {}).get("first_name", "") + " " + data.get("lead", {}).get("last_name", "") or
            "Unknown Lead"
        ).strip()

        email = (
            data.get("email") or
            data.get("lead_email") or
            data.get("lead", {}).get("email") or
            ""
        )

        company = (
            data.get("company") or
            data.get("company_name") or
            data.get("lead", {}).get("company") or
            ""
        )

        campaign_name = (
            data.get("campaign_name") or
            data.get("campaign") or
            data.get("campaign", {}).get("name") or
            ""
        )

        last_contacted = (
            data.get("last_contacted_date") or
            data.get("last_contacted") or
            data.get("updated_at") or
            data.get("lead", {}).get("last_contacted_at") or
            datetime.now().isoformat()
        )

        # Build task name
        task_name = f"{lead_name}"
        if company:
            task_name += f" - {company}"

        # Build task description
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
            "status": "new lead",
            "priority": 3,
            "notify_all": True
        }

        # Make the API request
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            task_data = response.json()

            success_msg = f"[SUCCESS] ClickUp task created for {lead_name} ({email})"
            print(success_msg)
            print(f"  Task ID: {task_data['id']}")
            print(f"  URL: {task_data['url']}")

            # Notify Slack
            if SLACK_WEBHOOK_URL:
                try:
                    slack_message = f"{success_msg}\nTask: {task_data.get('url', 'N/A')}"
                    requests.post(SLACK_WEBHOOK_URL, json={"text": slack_message})
                except:
                    pass

            return jsonify({
                "success": True,
                "message": "Lead created in ClickUp",
                "task_id": task_data.get("id"),
                "task_url": task_data.get("url"),
                "lead_data": {
                    "lead_name": lead_name,
                    "email": email,
                    "company": company,
                    "campaign_name": campaign_name,
                    "lead_status": lead_status,
                    "last_contacted_date": last_contacted
                }
            })
        else:
            error_msg = f"Failed to create task. Status: {response.status_code}, Response: {response.text}"
            print(f"[ERROR] {error_msg}")

            return jsonify({
                "success": False,
                "error": error_msg
            }), 500

    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        print(f"[ERROR] {error_msg}")
        return jsonify({
            "success": False,
            "error": error_msg
        }), 500

if __name__ == '__main__':
    print("="*60)
    print("Smartlead to ClickUp Webhook Server")
    print("="*60)
    print(f"ClickUp List ID: {CLICKUP_LIST_ID}")
    print(f"Server starting on http://localhost:5000")
    print("\nEndpoints:")
    print("  GET  /health              - Health check")
    print("  POST /smartlead-intake    - Smartlead webhook")
    print("\nTo expose this to the internet, use ngrok:")
    print("  ngrok http 5000")
    print("="*60)

    app.run(host='0.0.0.0', port=5000, debug=True)
