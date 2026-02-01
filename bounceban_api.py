"""
Bounce Ban API Wrapper
Handles bulk email verification via Bounce Ban's API.
Specializes in catch-all email verification that Million Verifier cannot reliably handle.
"""

import os
import time
import requests
from typing import Optional, Dict, List
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

BOUNCEBAN_API_KEY = os.getenv('BOUNCEBAN_API_KEY')

# API endpoints
BASE_URL = "https://api.bounceban.com"
BULK_VERIFY_URL = f"{BASE_URL}/v1/verify/bulk"
BULK_STATUS_URL = f"{BASE_URL}/v1/verify/bulk/status"
BULK_DUMP_URL = f"{BASE_URL}/v1/verify/bulk/dump"


@dataclass
class TaskStatus:
    task_id: str
    status: str  # 'importing', 'verifying', 'finished'
    total: int
    verified: int
    deliverable: int
    risky: int
    undeliverable: int
    unknown: int


@dataclass
class VerificationResult:
    email: str
    result: str  # 'deliverable', 'risky', 'undeliverable', 'unknown'
    score: int  # 0-100
    is_disposable: bool
    is_accept_all: bool
    is_role: bool
    is_free: bool


def get_api_key() -> str:
    """Get API key from environment."""
    if not BOUNCEBAN_API_KEY:
        raise ValueError("BOUNCEBAN_API_KEY not set in .env")
    return BOUNCEBAN_API_KEY


def create_bulk_task(emails: List[str], task_name: Optional[str] = None) -> Dict:
    """
    Create a bulk verification task.

    Args:
        emails: List of email addresses to verify
        task_name: Optional name for the task

    Returns:
        Dict with task_id and status
    """
    api_key = get_api_key()

    payload = {
        "emails": emails,
        "greylisting_bypass": "robust",  # Highest accuracy
        "mode": "regular"
    }

    if task_name:
        payload["name"] = task_name

    try:
        response = requests.post(
            BULK_VERIFY_URL,
            headers={
                "Authorization": api_key,
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=60
        )

        response.raise_for_status()
        data = response.json()

        if 'id' in data:
            return {
                'success': True,
                'task_id': data['id'],
                'status': data.get('status', 'importing')
            }
        else:
            return {
                'success': False,
                'error': data.get('error', 'Unknown error creating task')
            }

    except requests.exceptions.RequestException as e:
        return {
            'success': False,
            'error': f"Task creation failed: {str(e)}"
        }


def get_task_status(task_id: str) -> TaskStatus:
    """
    Check the status of a verification task.

    Args:
        task_id: The task ID returned from create_bulk_task

    Returns:
        TaskStatus with current progress
    """
    api_key = get_api_key()

    try:
        response = requests.get(
            f"{BULK_STATUS_URL}?id={task_id}",
            headers={"Authorization": api_key},
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        return TaskStatus(
            task_id=task_id,
            status=data.get('status', 'unknown'),
            total=data.get('total', 0),
            verified=data.get('verified', 0),
            deliverable=data.get('deliverable', 0),
            risky=data.get('risky', 0),
            undeliverable=data.get('undeliverable', 0),
            unknown=data.get('unknown', 0)
        )

    except requests.exceptions.RequestException as e:
        return TaskStatus(
            task_id=task_id,
            status='error',
            total=0,
            verified=0,
            deliverable=0,
            risky=0,
            undeliverable=0,
            unknown=0
        )


def download_results(task_id: str) -> List[VerificationResult]:
    """
    Download verification results.

    Args:
        task_id: The task ID to download results for

    Returns:
        List of VerificationResult objects
    """
    api_key = get_api_key()
    results = []
    cursor = None

    try:
        while True:
            # Build URL with cursor for pagination
            url = f"{BULK_DUMP_URL}?id={task_id}&retrieve_all=1"
            if cursor:
                url += f"&cursor={cursor}"

            response = requests.get(
                url,
                headers={"Authorization": api_key},
                timeout=120
            )

            response.raise_for_status()
            data = response.json()

            # Parse results (API returns 'items' not 'results')
            if 'items' in data:
                for item in data['items']:
                    results.append(VerificationResult(
                        email=item.get('email', ''),
                        result=item.get('result', 'unknown'),
                        score=item.get('score', 0),
                        is_disposable=item.get('is_disposable', False),
                        is_accept_all=item.get('is_accept_all', False),
                        is_role=item.get('is_role', False),
                        is_free=item.get('is_free', False)
                    ))

            # Check for pagination (cursor in response)
            cursor = data.get('cursor')
            if not cursor:
                break

        return results

    except requests.exceptions.RequestException as e:
        raise Exception(f"Download failed: {str(e)}")


def wait_for_completion(task_id: str, poll_interval: int = 10, timeout: int = 3600) -> TaskStatus:
    """
    Poll until verification is complete.

    Args:
        task_id: The task ID to wait for
        poll_interval: Seconds between status checks
        timeout: Maximum seconds to wait (default 60 minutes)

    Returns:
        Final TaskStatus
    """
    start_time = time.time()

    while True:
        status = get_task_status(task_id)

        if status.status == 'finished':
            return status

        if status.status == 'error':
            raise Exception(f"Verification error for task {task_id}")

        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise Exception(f"Timeout waiting for verification (task_id={task_id})")

        # Print progress
        if status.total > 0:
            percent = (status.verified / status.total) * 100
            print(f"  Progress: {percent:.1f}% ({status.verified}/{status.total}) - {status.status}")
        else:
            print(f"  Status: {status.status}")

        time.sleep(poll_interval)


def verify_emails(emails: List[str], wait: bool = True, poll_interval: int = 10, task_name: Optional[str] = None) -> Dict:
    """
    Full verification workflow: create task, wait, download.

    Args:
        emails: List of email addresses
        wait: If True, wait for completion. If False, return task_id immediately.
        poll_interval: Seconds between status checks when waiting
        task_name: Optional name for the task

    Returns:
        Dict with results or task_id for later retrieval
    """
    # Create task
    print(f"Uploading {len(emails)} emails to Bounce Ban...")
    task = create_bulk_task(emails, task_name=task_name)

    if not task['success']:
        return {
            'success': False,
            'error': task['error']
        }

    print(f"Task created. ID: {task['task_id']}")

    if not wait:
        return {
            'success': True,
            'task_id': task['task_id'],
            'status': 'submitted',
            'message': f'Use task_id {task["task_id"]} to check status or download results later'
        }

    # Wait for completion
    print("Waiting for verification to complete...")
    try:
        final_status = wait_for_completion(task['task_id'], poll_interval)
    except Exception as e:
        return {
            'success': False,
            'task_id': task['task_id'],
            'error': str(e)
        }

    # Download results
    print("Downloading results...")
    try:
        results = download_results(task['task_id'])
    except Exception as e:
        return {
            'success': False,
            'task_id': task['task_id'],
            'error': str(e)
        }

    # Build results dict keyed by email
    results_by_email = {}
    for r in results:
        results_by_email[r.email.lower()] = {
            'result': r.result,
            'score': r.score,
            'is_disposable': r.is_disposable,
            'is_accept_all': r.is_accept_all,
            'is_role': r.is_role,
            'is_free': r.is_free
        }

    # Count stats
    stats = {
        'deliverable': final_status.deliverable,
        'risky': final_status.risky,
        'undeliverable': final_status.undeliverable,
        'unknown': final_status.unknown
    }

    return {
        'success': True,
        'task_id': task['task_id'],
        'total': len(results),
        'results': results_by_email,
        'stats': stats
    }


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python bounceban_api.py test@example.com  # Verify single email")
        print("  python bounceban_api.py --status task_id  # Check job status")
        print("  python bounceban_api.py --download task_id  # Download results")
        sys.exit(1)

    if sys.argv[1] == '--status':
        task_id = sys.argv[2]
        status = get_task_status(task_id)
        print(f"Task ID: {status.task_id}")
        print(f"Status: {status.status}")
        if status.total > 0:
            percent = (status.verified / status.total) * 100
            print(f"Progress: {percent:.1f}% ({status.verified}/{status.total})")
        print(f"Deliverable: {status.deliverable}")
        print(f"Risky: {status.risky}")
        print(f"Undeliverable: {status.undeliverable}")
        print(f"Unknown: {status.unknown}")

    elif sys.argv[1] == '--download':
        task_id = sys.argv[2]
        results = download_results(task_id)
        for r in results[:10]:  # Show first 10
            print(f"{r.email}: {r.result} (score: {r.score})")
        if len(results) > 10:
            print(f"... and {len(results) - 10} more")

    else:
        # Single email test
        emails = sys.argv[1:]
        result = verify_emails(emails, wait=True)
        print(result)
