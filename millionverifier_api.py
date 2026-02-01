"""
Million Verifier API Wrapper
Handles bulk email verification via Million Verifier's API.
"""

import os
import time
import requests
import csv
import io
from typing import Optional, Dict, List
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

MILLIONVERIFIER_API_KEY = os.getenv('MILLIONVERIFIER_API_KEY')

# API endpoints
BASE_URL = "https://bulkapi.millionverifier.com/bulkapi/v2"
UPLOAD_URL = f"{BASE_URL}/upload"
FILEINFO_URL = f"{BASE_URL}/fileinfo"
DOWNLOAD_URL = f"{BASE_URL}/download"
FILELIST_URL = f"{BASE_URL}/filelist"


@dataclass
class UploadResult:
    success: bool
    file_id: Optional[int] = None
    error: Optional[str] = None


@dataclass
class FileStatus:
    file_id: int
    status: str  # 'finished', 'processing', 'waiting', 'error'
    total: int
    verified: int
    percent: float
    error: Optional[str] = None


@dataclass
class VerificationResult:
    email: str
    quality: str  # 'good', 'risky', 'bad'
    result: str   # 'ok', 'catch_all', 'unknown', 'invalid', 'disposable', etc.
    free: bool
    role: bool


def get_api_key() -> str:
    """Get API key from environment."""
    if not MILLIONVERIFIER_API_KEY:
        raise ValueError("MILLIONVERIFIER_API_KEY not set in .env")
    return MILLIONVERIFIER_API_KEY


def upload_emails(emails: List[str]) -> UploadResult:
    """
    Upload a list of emails for bulk verification.

    Args:
        emails: List of email addresses to verify

    Returns:
        UploadResult with file_id if successful
    """
    api_key = get_api_key()

    # Create CSV content
    csv_content = io.StringIO()
    writer = csv.writer(csv_content)
    writer.writerow(['email'])  # Header
    for email in emails:
        if email and email.strip():
            writer.writerow([email.strip()])

    csv_content.seek(0)
    csv_bytes = csv_content.getvalue().encode('utf-8')

    try:
        response = requests.post(
            f"{UPLOAD_URL}?key={api_key}",
            files={'file_contents': ('emails.csv', csv_bytes, 'text/csv')},
            timeout=60
        )

        response.raise_for_status()
        data = response.json()

        if 'file_id' in data:
            return UploadResult(
                success=True,
                file_id=data['file_id']
            )
        else:
            return UploadResult(
                success=False,
                error=data.get('error', 'Unknown error')
            )

    except requests.exceptions.RequestException as e:
        return UploadResult(
            success=False,
            error=f"Upload failed: {str(e)}"
        )


def get_file_status(file_id: int) -> FileStatus:
    """
    Check the status of a verification job.

    Args:
        file_id: The file ID returned from upload

    Returns:
        FileStatus with current progress
    """
    api_key = get_api_key()

    try:
        response = requests.get(
            f"{FILEINFO_URL}?key={api_key}&file_id={file_id}",
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        return FileStatus(
            file_id=file_id,
            status=data.get('status', 'unknown'),
            total=data.get('total', 0),
            verified=data.get('verified', 0),
            percent=data.get('percent', 0.0)
        )

    except requests.exceptions.RequestException as e:
        return FileStatus(
            file_id=file_id,
            status='error',
            total=0,
            verified=0,
            percent=0.0,
            error=str(e)
        )


def download_results(file_id: int, filter_type: str = 'all') -> List[VerificationResult]:
    """
    Download verification results.

    Args:
        file_id: The file ID to download results for
        filter_type: 'all', 'ok', 'catch_all', 'unknown', 'invalid', 'disposable'

    Returns:
        List of VerificationResult objects
    """
    api_key = get_api_key()

    try:
        response = requests.get(
            f"{DOWNLOAD_URL}?key={api_key}&file_id={file_id}&filter={filter_type}",
            timeout=120
        )

        response.raise_for_status()

        # Parse CSV response
        results = []
        content = response.text
        reader = csv.DictReader(io.StringIO(content))

        for row in reader:
            results.append(VerificationResult(
                email=row.get('email', ''),
                quality=row.get('quality', 'unknown'),
                result=row.get('result', 'unknown'),
                free=row.get('free', '').lower() == 'true',
                role=row.get('role', '').lower() == 'true'
            ))

        return results

    except requests.exceptions.RequestException as e:
        raise Exception(f"Download failed: {str(e)}")


def wait_for_completion(file_id: int, poll_interval: int = 10, timeout: int = 1800) -> FileStatus:
    """
    Poll until verification is complete.

    Args:
        file_id: The file ID to wait for
        poll_interval: Seconds between status checks
        timeout: Maximum seconds to wait (default 30 minutes)

    Returns:
        Final FileStatus
    """
    start_time = time.time()

    while True:
        status = get_file_status(file_id)

        if status.status == 'finished':
            return status

        if status.status == 'error':
            raise Exception(f"Verification error: {status.error}")

        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise Exception(f"Timeout waiting for verification (file_id={file_id})")

        print(f"  Progress: {status.percent:.1f}% ({status.verified}/{status.total}) - {status.status}")
        time.sleep(poll_interval)


def verify_emails(emails: List[str], wait: bool = True, poll_interval: int = 10) -> Dict:
    """
    Full verification workflow: upload, wait, download.

    Args:
        emails: List of email addresses
        wait: If True, wait for completion. If False, return file_id immediately.
        poll_interval: Seconds between status checks when waiting

    Returns:
        Dict with results or file_id for later retrieval
    """
    # Upload
    print(f"Uploading {len(emails)} emails to Million Verifier...")
    upload = upload_emails(emails)

    if not upload.success:
        return {
            'success': False,
            'error': upload.error
        }

    print(f"Upload complete. File ID: {upload.file_id}")

    if not wait:
        return {
            'success': True,
            'file_id': upload.file_id,
            'status': 'submitted',
            'message': f'Use file_id {upload.file_id} to check status or download results later'
        }

    # Wait for completion
    print("Waiting for verification to complete...")
    try:
        final_status = wait_for_completion(upload.file_id, poll_interval)
    except Exception as e:
        return {
            'success': False,
            'file_id': upload.file_id,
            'error': str(e)
        }

    # Download results
    print("Downloading results...")
    try:
        results = download_results(upload.file_id)
    except Exception as e:
        return {
            'success': False,
            'file_id': upload.file_id,
            'error': str(e)
        }

    # Build results dict keyed by email
    results_by_email = {}
    for r in results:
        results_by_email[r.email.lower()] = {
            'quality': r.quality,
            'result': r.result,
            'free': r.free,
            'role': r.role
        }

    # Count stats
    stats = {
        'ok': 0,
        'catch_all': 0,
        'unknown': 0,
        'invalid': 0,
        'disposable': 0,
        'other': 0
    }
    for r in results:
        if r.result in stats:
            stats[r.result] += 1
        else:
            stats['other'] += 1

    return {
        'success': True,
        'file_id': upload.file_id,
        'total': len(results),
        'results': results_by_email,
        'stats': stats
    }


def list_files() -> List[Dict]:
    """List all verification files in the account."""
    api_key = get_api_key()

    try:
        response = requests.get(
            f"{FILELIST_URL}?key={api_key}",
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return [{'error': str(e)}]


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python millionverifier_api.py test@example.com  # Verify single email")
        print("  python millionverifier_api.py --status 12345    # Check job status")
        print("  python millionverifier_api.py --list            # List all files")
        sys.exit(1)

    if sys.argv[1] == '--status':
        file_id = int(sys.argv[2])
        status = get_file_status(file_id)
        print(f"File ID: {status.file_id}")
        print(f"Status: {status.status}")
        print(f"Progress: {status.percent:.1f}% ({status.verified}/{status.total})")

    elif sys.argv[1] == '--list':
        files = list_files()
        for f in files:
            print(f)

    elif sys.argv[1] == '--download':
        file_id = int(sys.argv[2])
        results = download_results(file_id)
        for r in results[:10]:  # Show first 10
            print(f"{r.email}: {r.result} ({r.quality})")
        if len(results) > 10:
            print(f"... and {len(results) - 10} more")

    else:
        # Single email test
        emails = sys.argv[1:]
        result = verify_emails(emails, wait=True)
        print(result)
