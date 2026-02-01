#!/usr/bin/env python3
"""
Onboard a new client by sending welcome email
Usage: python onboard_client.py <client_email> [--name CLIENT_NAME] [--calendar-link LINK]
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Import send_email functions
from send_email import send_email_smtp, send_email_sendgrid

load_dotenv()


def load_template(template_name='welcome_email.html'):
    """Load email template from templates directory"""
    template_path = Path(__file__).parent / 'templates' / template_name

    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()


def render_template(template_content, **variables):
    """Replace template variables with actual values"""
    rendered = template_content
    for key, value in variables.items():
        rendered = rendered.replace(f"{{{key}}}", value)
    return rendered


def onboard_client(client_email, client_name=None, calendar_link=None):
    """Send welcome email to new client"""

    # Get defaults from environment or use placeholders
    client_name = client_name or os.getenv('DEFAULT_CLIENT_NAME', 'there')
    calendar_link = calendar_link or os.getenv('CALENDAR_LINK', 'https://calendly.com/sevengravity')

    print(f"Onboarding client: {client_email}")
    print(f"Client name: {client_name}")
    print(f"Calendar link: {calendar_link}")

    # Load and render template
    try:
        template = load_template()
        email_body = render_template(
            template,
            client_name=client_name,
            calendar_link=calendar_link
        )
    except Exception as e:
        print(f"✗ Failed to load template: {e}")
        return False

    # Send email
    subject = "Welcome to Seven Gravity - Let's Get Started!"
    email_method = os.getenv('EMAIL_METHOD', 'smtp').lower()

    try:
        if email_method == 'sendgrid':
            success = send_email_sendgrid(client_email, subject, email_body)
        else:
            success = send_email_smtp(client_email, subject, email_body)

        if success:
            print(f"\n✓ Successfully onboarded {client_email}")
            print(f"✓ Welcome email sent with calendar link: {calendar_link}")
            return True
        else:
            print(f"\n✗ Failed to onboard {client_email}")
            return False

    except Exception as e:
        print(f"✗ Error sending email: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Onboard a new client')
    parser.add_argument('client_email', help='Client email address')
    parser.add_argument('--name', dest='client_name', help='Client name (optional)')
    parser.add_argument('--calendar-link', dest='calendar_link', help='Kickoff call calendar link (optional)')

    args = parser.parse_args()

    success = onboard_client(
        args.client_email,
        client_name=args.client_name,
        calendar_link=args.calendar_link
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
