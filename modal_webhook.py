"""
Modal webhook handler for Seven Gravity automation system.
Provides event-driven execution via HTTP webhooks for Smartlead to ClickUp integration.
"""

import modal
import os
from datetime import datetime

# Create Modal app
app = modal.App("claude-orchestrator")

# Create image with required dependencies
image = (
    modal.Image.debian_slim()
    .pip_install("requests", "python-dotenv", "fastapi[standard]")
)

# Deploy FastAPI app to Modal
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("seven-gravity-env")]
)
@modal.asgi_app()
def fastapi_app():
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse

    web_app = FastAPI()

    @web_app.get("/list-webhooks")
    async def list_webhooks():
        """List all available webhooks."""
        webhooks = {
            "smartlead-intake": {
                "directive": "directives/smartlead_to_clickup.md",
                "description": "Create ClickUp task when Smartlead lead status is interested/meeting request",
                "allowed_tools": ["create_clickup_task"]
            }
        }

        return {
            "success": True,
            "webhooks": webhooks,
            "count": len(webhooks)
        }

    @web_app.post("/smartlead-intake")
    async def smartlead_intake(request: Request):
        """
        Handle Smartlead webhook - accepts whatever payload Smartlead sends.
        No custom request body configuration needed in Smartlead.
        """
        try:
            data = await request.json()

            # Log the entire payload for debugging
            print("="*60)
            print("[WEBHOOK] Received payload from Smartlead:")
            import json as json_lib
            print(json_lib.dumps(data, indent=2))
            print("="*60)

            # Log webhook received
            log_message = f"[WEBHOOK] Received Smartlead webhook at {datetime.now().isoformat()}"
            print(log_message)

            # Send Slack notification if configured
            slack_url = os.getenv("SLACK_WEBHOOK_URL")
            if slack_url:
                try:
                    import requests
                    requests.post(slack_url, json={"text": log_message})
                except:
                    pass

            # Process the lead
            result = handle_smartlead_intake(data)
            return JSONResponse(result)

        except Exception as e:
            error_msg = f"Error executing webhook: {str(e)}"
            print(error_msg)

            # Send error to Slack
            slack_url = os.getenv("SLACK_WEBHOOK_URL")
            if slack_url:
                try:
                    import requests
                    requests.post(slack_url, json={"text": f"[ERROR] {error_msg}"})
                except:
                    pass

            return JSONResponse({
                "success": False,
                "error": error_msg
            })

    def handle_smartlead_intake(data: dict):
        """
        Handle Smartlead lead status change webhook.
        Creates ClickUp task for leads with 'interested' or 'meeting request' status.
        Flexible field extraction - works with any Smartlead payload format.
        """
        import requests

        try:
            # Get API keys from environment
            clickup_api_key = os.getenv("CLICKUP_API_KEY")
            clickup_list_id = os.getenv("CLICKUP_LIST_ID")

            if not clickup_api_key or not clickup_list_id:
                return {
                    "success": False,
                    "error": "Missing ClickUp API credentials in environment"
                }

            # Flexible field extraction - try multiple possible field names
            # Smartlead might use different naming conventions
            lead_status = (
                data.get("lead_status") or
                data.get("status") or
                data.get("lead", {}).get("status") or
                ""
            ).lower()

            # Only process if status is interested or meeting request
            if lead_status and lead_status not in ["interested", "meeting request"]:
                print(f"[INFO] Skipping lead - status '{lead_status}' not in trigger list")
                return {
                    "success": False,
                    "message": f"Status '{lead_status}' does not trigger ClickUp creation."
                }

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
            url = f"https://api.clickup.com/api/v2/list/{clickup_list_id}/task"

            headers = {
                "Authorization": clickup_api_key,
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
                slack_url = os.getenv("SLACK_WEBHOOK_URL")
                if slack_url:
                    try:
                        slack_message = f"{success_msg}\nTask: {task_data.get('url', 'N/A')}"
                        requests.post(slack_url, json={"text": slack_message})
                    except:
                        pass

                return {
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
                }
            else:
                error_msg = f"Failed to create task. Status: {response.status_code}, Response: {response.text}"
                print(f"[ERROR] {error_msg}")

                return {
                    "success": False,
                    "error": error_msg
                }

        except Exception as e:
            return {
                "success": False,
                "error": f"Exception in handle_smartlead_intake: {str(e)}"
            }

    @web_app.post("/test")
    async def test():
        """Test endpoint to verify webhook is working."""
        try:
            return {
                "success": True,
                "message": "Webhook is working! All systems operational.",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }

    return web_app
