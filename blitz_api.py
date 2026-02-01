"""
BlitzAPI Python Wrapper

B2B data enrichment API for:
- People search (Waterfall ICP, Employee Finder)
- Email enrichment
- Company enrichment (Domain to LinkedIn)

Docs: https://docs.blitz-api.ai/
"""

import os
import requests
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# Configuration
BASE_URL = "https://api.blitz-api.ai"
API_KEY = os.getenv("BLITZ_API_KEY")

# Default cascade for Waterfall ICP Search (VP priority)
DEFAULT_CASCADE = [
    {
        "include_title": ["VP Sales", "VP Business Development", "VP Growth", "VP Marketing",
                          "Vice President Sales", "Vice President Marketing", "Vice President Growth"],
        "exclude_title": ["Assistant", "Coordinator", "Intern"],
        "location": ["WORLD"],
        "include_headline_search": True
    },
    {
        "include_title": ["CEO", "Founder", "Co-Founder", "President", "Owner"],
        "exclude_title": ["Assistant"],
        "location": ["WORLD"],
        "include_headline_search": True
    },
    {
        "include_title": ["CMO", "CRO", "Chief Marketing Officer", "Chief Revenue Officer"],
        "exclude_title": ["Assistant"],
        "location": ["WORLD"],
        "include_headline_search": True
    },
    {
        "include_title": ["Director Sales", "Director Marketing", "Director Growth",
                          "Head of Sales", "Head of Marketing", "Head of Growth"],
        "exclude_title": ["Assistant", "Coordinator"],
        "location": ["WORLD"],
        "include_headline_search": True
    }
]


@dataclass
class Person:
    """Represents a person/contact from BlitzAPI"""
    linkedin_url: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    headline: Optional[str] = None
    title: Optional[str] = None
    location: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    company_name: Optional[str] = None
    connection_count: Optional[int] = None
    profile_picture: Optional[str] = None
    icp_rank: Optional[int] = None
    what_matched: Optional[List[str]] = None


@dataclass
class EmailResult:
    """Result from email enrichment"""
    found: bool
    email: Optional[str] = None
    all_emails: List[Dict] = field(default_factory=list)


@dataclass
class CompanyResult:
    """Result from company enrichment"""
    found: bool
    company_linkedin_url: Optional[str] = None
    domain: Optional[str] = None


@dataclass
class ApiKeyInfo:
    """API key information"""
    valid: bool
    remaining_credits: float
    max_requests_per_second: int
    next_reset_at: Optional[str] = None
    active_plans: List[Dict] = field(default_factory=list)


class BlitzAPIError(Exception):
    """Custom exception for BlitzAPI errors"""
    def __init__(self, message: str, status_code: int = None, response: dict = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)


class BlitzAPI:
    """
    BlitzAPI client for B2B data enrichment.

    Usage:
        api = BlitzAPI()

        # Search for decision-makers at a company
        people = api.waterfall_icp_search(
            company_linkedin_url="https://www.linkedin.com/company/acme",
            max_results=3
        )

        # Get email for a person
        email = api.find_work_email(
            person_linkedin_url="https://www.linkedin.com/in/johndoe"
        )
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or API_KEY
        if not self.api_key:
            raise BlitzAPIError("BLITZ_API_KEY not found. Set it in .env or pass to constructor.")

        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        })

    def _request(self, method: str, endpoint: str, data: dict = None, retries: int = 3) -> dict:
        """Make API request with retry logic"""
        url = f"{BASE_URL}{endpoint}"

        for attempt in range(retries):
            try:
                if method == "GET":
                    response = self.session.get(url, timeout=30)
                else:
                    response = self.session.post(url, json=data, timeout=60)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    raise BlitzAPIError("Invalid API key", 401, response.json())
                elif response.status_code == 402:
                    raise BlitzAPIError("Insufficient credits", 402, response.json())
                elif response.status_code == 422:
                    raise BlitzAPIError(f"Invalid input: {response.json()}", 422, response.json())
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    wait_time = 2 ** attempt
                    print(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise BlitzAPIError(f"API error: {response.status_code}", response.status_code)

            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    print(f"Timeout, retrying ({attempt + 1}/{retries})...")
                    time.sleep(2)
                    continue
                raise BlitzAPIError("Request timed out after retries")
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    print(f"Request error, retrying ({attempt + 1}/{retries})...")
                    time.sleep(2)
                    continue
                raise BlitzAPIError(f"Request failed: {str(e)}")

        raise BlitzAPIError("Max retries exceeded")

    # ==================== Account ====================

    def get_key_info(self) -> ApiKeyInfo:
        """
        Get API key details including remaining credits.
        Cost: 0 credits
        """
        data = self._request("GET", "/v2/account/key-info")
        return ApiKeyInfo(
            valid=data.get("valid", False),
            remaining_credits=data.get("remaining_credits", 0),
            max_requests_per_second=data.get("max_requests_per_seconds", 5),
            next_reset_at=data.get("next_reset_at"),
            active_plans=data.get("active_plans", [])
        )

    # ==================== People Search ====================

    def waterfall_icp_search(
        self,
        company_linkedin_url: str,
        cascade: List[Dict] = None,
        max_results: int = 3
    ) -> List[Person]:
        """
        Search for decision-makers at a company using Waterfall ICP.

        Searches hierarchically: VP > CEO/Founder > CMO/CRO > Director

        Args:
            company_linkedin_url: LinkedIn company URL (e.g., "https://www.linkedin.com/company/acme")
            cascade: Custom cascade config (uses DEFAULT_CASCADE if None)
            max_results: Maximum results to return (default: 3)

        Returns:
            List of Person objects

        Cost: 1 credit per result
        """
        data = {
            "company_linkedin_url": company_linkedin_url,
            "cascade": cascade or DEFAULT_CASCADE,
            "max_results": max_results
        }

        response = self._request("POST", "/v2/search/waterfall-icp", data)

        people = []
        for result in response.get("results", []):
            person_data = result.get("person", {})
            people.append(Person(
                linkedin_url=person_data.get("linkedin_url", ""),
                first_name=person_data.get("first_name"),
                last_name=person_data.get("last_name"),
                full_name=person_data.get("full_name"),
                headline=person_data.get("headline"),
                title=person_data.get("headline"),  # headline often contains title
                location=person_data.get("location"),
                company_linkedin_url=company_linkedin_url,
                connection_count=person_data.get("connection_count"),
                profile_picture=person_data.get("profile_picture"),
                icp_rank=result.get("ranking"),
                what_matched=result.get("what_matched", [])
            ))

        return people

    def employee_finder(
        self,
        company_linkedin_url: str,
        job_level: List[str] = None,
        job_function: List[str] = None,
        country_code: List[str] = None,
        max_results: int = 3,
        page: int = 1
    ) -> List[Person]:
        """
        Find employees at a company with filters.

        Args:
            company_linkedin_url: LinkedIn company URL
            job_level: Filter by level ["C-Team", "VP", "Director", "Manager", "Staff", "Other"]
            job_function: Filter by function ["Sales", "Marketing", "Engineering", etc.]
            country_code: Filter by country ["US", "UK", "CA", etc.]
            max_results: Results per page (default: 3)
            page: Page number for pagination

        Returns:
            List of Person objects

        Cost: 1 credit per result
        """
        data = {
            "company_linkedin_url": company_linkedin_url,
            "max_results": max_results,
            "page": page
        }

        if job_level:
            data["job_level"] = job_level
        if job_function:
            data["job_function"] = job_function
        if country_code:
            data["country_code"] = country_code

        response = self._request("POST", "/v2/search/employee-finder", data)

        people = []
        for result in response.get("results", []):
            people.append(Person(
                linkedin_url=result.get("linkedin_url", ""),
                first_name=result.get("first_name"),
                last_name=result.get("last_name"),
                full_name=result.get("full_name"),
                headline=result.get("headline"),
                title=result.get("headline"),
                location=result.get("location"),
                company_linkedin_url=company_linkedin_url,
                company_name=result.get("company_name"),
                connection_count=result.get("connection_count"),
                profile_picture=result.get("profile_picture")
            ))

        return people

    # ==================== People Enrichment ====================

    def find_work_email(self, person_linkedin_url: str) -> EmailResult:
        """
        Find work email for a LinkedIn profile.

        Args:
            person_linkedin_url: LinkedIn profile URL

        Returns:
            EmailResult with found status and email(s)

        Cost: 1 credit (on success)
        """
        data = {"person_linkedin_url": person_linkedin_url}

        response = self._request("POST", "/v2/enrichment/email", data)

        return EmailResult(
            found=response.get("found", False),
            email=response.get("email"),
            all_emails=response.get("all_emails", [])
        )

    # ==================== Company Enrichment ====================

    def domain_to_linkedin(self, domain: str) -> CompanyResult:
        """
        Get LinkedIn company URL from domain.

        Args:
            domain: Company domain (e.g., "acme.com" or "https://acme.com")

        Returns:
            CompanyResult with found status and LinkedIn URL

        Cost: 1 credit (on success)
        """
        data = {"domain": domain}

        response = self._request("POST", "/v2/enrichment/domain-to-linkedin", data)

        return CompanyResult(
            found=response.get("found", False),
            company_linkedin_url=response.get("company_linkedin_url"),
            domain=domain
        )

    # ==================== Convenience Methods ====================

    def search_decision_makers(
        self,
        company_linkedin_url: str = None,
        company_domain: str = None,
        company_size: int = None,
        with_email: bool = True
    ) -> List[Dict]:
        """
        Find decision-makers at a company with optional email enrichment.

        Automatically determines contact count based on company size:
        - 1-20 employees: 1-2 contacts
        - 21-100 employees: 2-3 contacts
        - 100+ employees: 3-4 contacts

        Args:
            company_linkedin_url: LinkedIn company URL (required if domain not provided)
            company_domain: Company domain (will be converted to LinkedIn URL)
            company_size: Employee count (for determining max contacts)
            with_email: Whether to enrich with emails (default: True)

        Returns:
            List of dicts with person info and optional email
        """
        # Get LinkedIn URL from domain if needed
        if not company_linkedin_url and company_domain:
            result = self.domain_to_linkedin(company_domain)
            if not result.found:
                return []
            company_linkedin_url = result.company_linkedin_url

        if not company_linkedin_url:
            raise BlitzAPIError("Either company_linkedin_url or company_domain required")

        # Determine max contacts based on company size
        if company_size:
            if company_size <= 20:
                max_results = 2
            elif company_size <= 100:
                max_results = 3
            else:
                max_results = 4
        else:
            max_results = 3  # Default

        # Search for decision-makers
        people = self.waterfall_icp_search(
            company_linkedin_url=company_linkedin_url,
            max_results=max_results
        )

        results = []
        for person in people:
            contact = {
                "linkedin_url": person.linkedin_url,
                "first_name": person.first_name,
                "last_name": person.last_name,
                "full_name": person.full_name,
                "title": person.headline,
                "location": person.location,
                "company_linkedin_url": company_linkedin_url,
                "icp_rank": person.icp_rank,
                "what_matched": person.what_matched,
                "email": None,
                "email_found": False
            }

            # Enrich with email if requested
            if with_email and person.linkedin_url:
                try:
                    email_result = self.find_work_email(person.linkedin_url)
                    contact["email"] = email_result.email
                    contact["email_found"] = email_result.found
                    contact["all_emails"] = email_result.all_emails
                except BlitzAPIError as e:
                    print(f"Warning: Could not get email for {person.full_name}: {e.message}")

            results.append(contact)

        return results


# ==================== Standalone Functions ====================

def check_credits() -> float:
    """Check remaining BlitzAPI credits"""
    api = BlitzAPI()
    info = api.get_key_info()
    print(f"Remaining credits: {info.remaining_credits}")
    print(f"Rate limit: {info.max_requests_per_second} req/s")
    if info.active_plans:
        print(f"Plan: {info.active_plans[0].get('name', 'Unknown')}")
    return info.remaining_credits


def search_company(
    domain: str = None,
    linkedin_url: str = None,
    company_size: int = None,
    with_email: bool = True
) -> List[Dict]:
    """
    Search for decision-makers at a company.

    Usage:
        # By domain
        results = search_company(domain="acme.com", company_size=50)

        # By LinkedIn URL
        results = search_company(linkedin_url="https://www.linkedin.com/company/acme")
    """
    api = BlitzAPI()
    return api.search_decision_makers(
        company_linkedin_url=linkedin_url,
        company_domain=domain,
        company_size=company_size,
        with_email=with_email
    )


if __name__ == "__main__":
    # Test API connection
    try:
        credits = check_credits()
        print(f"\nBlitzAPI connected successfully!")
        print(f"Credits available: {credits}")
    except BlitzAPIError as e:
        print(f"Error: {e.message}")
        if "API_KEY" in e.message:
            print("Please add BLITZ_API_KEY to your .env file")
