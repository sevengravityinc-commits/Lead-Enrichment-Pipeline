#!/usr/bin/env python3
"""
Send email via SMTP or SendGrid API
Usage: python send_email.py <to_email> <subject> <body_html> [--from-email FROM] [--from-name NAME]
"""

import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()


def send_email_smtp(to_email, subject, body_html, from_email=None, from_name=None):
    """Send email using SMTP"""
    smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP_USER and SMTP_PASSWORD must be set in .env")

    from_email = from_email or smtp_user
    from_name = from_name or os.getenv('FROM_NAME', 'Seven Gravity')

    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f"{from_name} <{from_email}>"
    msg['To'] = to_email

    # Attach HTML body
    html_part = MIMEText(body_html, 'html')
    msg.attach(html_part)

    # Send email
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        print(f"✓ Email sent successfully to {to_email}")
        return True
    except Exception as e:
        print(f"✗ Failed to send email: {e}")
        return False


def send_email_sendgrid(to_email, subject, body_html, from_email=None, from_name=None):
    """Send email using SendGrid API"""
    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail
    except ImportError:
        print("SendGrid not installed. Install with: pip install sendgrid")
        return False

    api_key = os.getenv('SENDGRID_API_KEY')
    if not api_key:
        raise ValueError("SENDGRID_API_KEY must be set in .env")

    from_email = from_email or os.getenv('FROM_EMAIL', 'hello@sevengravity.com')
    from_name = from_name or os.getenv('FROM_NAME', 'Seven Gravity')

    message = Mail(
        from_email=(from_email, from_name),
        to_emails=to_email,
        subject=subject,
        html_content=body_html
    )

    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        print(f"✓ Email sent successfully to {to_email} (Status: {response.status_code})")
        return True
    except Exception as e:
        print(f"✗ Failed to send email: {e}")
        return False


def main():
    if len(sys.argv) < 4:
        print("Usage: python send_email.py <to_email> <subject> <body_html>")
        sys.exit(1)

    to_email = sys.argv[1]
    subject = sys.argv[2]
    body_html = sys.argv[3]

    # Check which email method to use
    email_method = os.getenv('EMAIL_METHOD', 'smtp').lower()

    if email_method == 'sendgrid':
        success = send_email_sendgrid(to_email, subject, body_html)
    else:
        success = send_email_smtp(to_email, subject, body_html)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
