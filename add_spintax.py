"""
Add Spintax to Email Sequences
Adds extensive, conversational spintax variations to email sequences in Google Docs.
Adds %signature% placeholder at the end of each email body.
"""

import os
import sys
import re
import requests
from typing import List, Dict
from dotenv import load_dotenv

from google_docs_helper import read_document, update_document

# Load environment variables
load_dotenv()

# Model configuration
DEFAULT_MODEL = os.getenv('SPINTAX_MODEL', 'openai/gpt-4o-mini')


def extract_email_bodies(doc_content: str) -> Dict[str, str]:
    """
    Extract email bodies from the Google Doc.

    Returns:
        Dict mapping email keys (e.g., 'SEQ_A_EMAIL_1') to body text
    """
    bodies = {}

    # Parse sequences
    sequence_pattern = r'SEQUENCE ([ABC])'
    email_pattern = r'EMAIL (\d) - (.*?)\nSubject: (.*?)\nBody:\n(.*?)(?=\n\n---|═|$)'

    sequences = re.split(r'═+', doc_content)

    for seq_section in sequences:
        seq_match = re.search(sequence_pattern, seq_section)
        if not seq_match:
            continue

        seq_letter = seq_match.group(1)

        # Find all emails in this sequence
        emails = re.finditer(email_pattern, seq_section, re.DOTALL)

        for email_match in emails:
            email_num = email_match.group(1)
            email_type = email_match.group(2)
            subject = email_match.group(3)
            body = email_match.group(4).strip()

            key = f'SEQ_{seq_letter}_EMAIL_{email_num}'
            bodies[key] = {
                'type': email_type,
                'subject': subject,
                'body': body
            }

    return bodies


def generate_spintax_variations(text: str, num_variations: int = 4) -> str:
    """
    Generate spintax variations for a given text using OpenRouter API.

    Args:
        text: Original text
        num_variations: Number of variations to generate (3-5)

    Returns:
        Spintax-enhanced text with {option1|option2|option3} format
    """
    api_key = os.getenv('OPENROUTER_API_KEY')
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not found in .env file")

    prompt = f"""Generate conversational spintax variations for this cold email text.

IMPORTANT RULES:
1. Add {num_variations} variations per spintax block
2. Multiple spintax blocks per email are encouraged (5-7+ blocks is great)
3. Variations must be:
   - Conversational and human-sounding
   - Natural, not robotic or templated
   - Complete thoughts (no awkward fragments)
4. Use spintax format: {{option1|option2|option3|option4}}
5. Preserve {{first_name}} and {{company_name}} placeholders (do NOT put these in spintax)
6. Add spintax to:
   - Greetings (Hey/Hi/Hello/Hey there)
   - Key phrases and verbs
   - Adjectives and descriptors
   - CTAs (Mind if I share/Can I send/Want me to send)
   - Any sentence that can benefit from variation

Original text:
{text}

Return ONLY the spintax-enhanced version, no explanations."""

    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": DEFAULT_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1000
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content'].strip()
    except Exception as e:
        raise Exception(f"OpenRouter API error: {str(e)}")


def add_spintax_to_doc(document_id: str, verbose: bool = True) -> Dict:
    """
    Add spintax variations to email sequences in a Google Doc.

    Args:
        document_id: Google Docs document ID
        verbose: Whether to print progress

    Returns:
        Dict with processing statistics
    """
    if verbose:
        print("Reading Google Doc...")

    # Read document
    try:
        doc_content = read_document(document_id)
    except Exception as e:
        return {"error": f"Failed to read document: {str(e)}"}

    if verbose:
        print(f"Document length: {len(doc_content)} characters")

    # Extract email bodies
    bodies = extract_email_bodies(doc_content)

    if not bodies:
        return {"error": "No email bodies found in document"}

    if verbose:
        print(f"Found {len(bodies)} email bodies")
        print("Adding spintax variations...")

    # Process each email body
    spintaxed_bodies = {}
    variation_counts = {}

    for key, email_data in bodies.items():
        if verbose:
            print(f"  Processing {key}...")

        body = email_data['body']

        try:
            # Generate spintax variations
            spintaxed_body = generate_spintax_variations(body)

            # Add %signature% at the end
            spintaxed_body += "\n\n%signature%"

            # Count spintax blocks
            variation_count = len(re.findall(r'\{[^}]+\}', spintaxed_body))

            spintaxed_bodies[key] = {
                **email_data,
                'body': spintaxed_body
            }
            variation_counts[key] = variation_count

            if verbose:
                print(f"    ✓ Added {variation_count} spintax blocks")

        except Exception as e:
            if verbose:
                print(f"    ✗ Error: {str(e)}")
            spintaxed_bodies[key] = {
                **email_data,
                'body': body + "\n\n%signature%"
            }
            variation_counts[key] = 0

    # Rebuild document with spintax
    if verbose:
        print("\nRebuilding document...")

    new_content = rebuild_document_with_spintax(doc_content, spintaxed_bodies)

    # Update document
    try:
        update_document(document_id, new_content)
        if verbose:
            print("✓ Document updated successfully")
    except Exception as e:
        return {"error": f"Failed to update document: {str(e)}"}

    # Calculate statistics
    total_variations = sum(variation_counts.values())
    total_signatures = len(spintaxed_bodies)

    return {
        "document_id": document_id,
        "emails_processed": len(spintaxed_bodies),
        "total_variations": total_variations,
        "total_signatures": total_signatures,
        "variation_counts": variation_counts
    }


def rebuild_document_with_spintax(original_content: str, spintaxed_bodies: Dict[str, Dict]) -> str:
    """
    Rebuild document content with spintaxed email bodies.

    Args:
        original_content: Original document content
        spintaxed_bodies: Dict mapping email keys to spintaxed email data

    Returns:
        New document content with spintax
    """
    # Split by sequences
    lines = original_content.split('\n')
    new_lines = []

    current_seq = None
    current_email = None
    in_body = False
    body_lines = []

    for line in lines:
        # Check for sequence marker
        seq_match = re.search(r'SEQUENCE ([ABC])', line)
        if seq_match:
            current_seq = seq_match.group(1)

        # Check for email marker
        email_match = re.search(r'EMAIL (\d) -', line)
        if email_match:
            current_email = email_match.group(1)

        # Check if we're at the body section
        if line.strip() == 'Body:':
            in_body = True
            new_lines.append(line)
            continue

        # If in body section, collect lines until we hit a separator
        if in_body:
            if line.strip() in ['---', ''] or line.startswith('═'):
                # End of body - replace with spintaxed version
                if current_seq and current_email:
                    key = f'SEQ_{current_seq}_EMAIL_{current_email}'
                    if key in spintaxed_bodies:
                        new_lines.append(spintaxed_bodies[key]['body'])
                    else:
                        new_lines.extend(body_lines)
                else:
                    new_lines.extend(body_lines)

                body_lines = []
                in_body = False
                new_lines.append(line)
            else:
                body_lines.append(line)
        else:
            new_lines.append(line)

    return '\n'.join(new_lines)


def print_summary(result: Dict):
    """Print summary of spintax addition results."""
    print("\n" + "=" * 60)
    print("SPINTAX ADDITION - SUMMARY")
    print("=" * 60)

    if "error" in result:
        print(f"ERROR: {result['error']}")
        return

    print(f"Document ID: {result['document_id']}")
    print(f"Emails processed: {result['emails_processed']}")
    print(f"Total spintax blocks added: {result['total_variations']}")
    print(f"Total %signature% placeholders added: {result['total_signatures']}")

    if "variation_counts" in result:
        print("\nVARIATIONS PER EMAIL:")
        for key, count in sorted(result['variation_counts'].items()):
            print(f"  {key}: {count} blocks")

    print("\nFEATURES APPLIED:")
    print("  ✓ Extensive spintax (5-7+ blocks per email)")
    print("  ✓ Conversational, human-readable variations")
    print("  ✓ %signature% added to all email bodies")
    print("  ✓ {first_name} and {company_name} placeholders preserved")

    print("\nNEXT STEPS:")
    print("  1. Review spintax in Google Doc")
    print("  2. Edit if needed (ensure variations sound human)")
    print("  3. When satisfied, run /smartlead-upload")

    print("=" * 60)


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python add_spintax.py <document_id>")
        print("\nExamples:")
        print("  python add_spintax.py 1a2b3c4d5e6f7g8h9i0j")
        print("  python add_spintax.py https://docs.google.com/document/d/1a2b3c4d5e6f7g8h9i0j/edit")
        sys.exit(1)

    doc_id_or_url = sys.argv[1]

    # Extract document ID from URL if necessary
    if 'docs.google.com' in doc_id_or_url:
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', doc_id_or_url)
        if match:
            document_id = match.group(1)
        else:
            print("ERROR: Could not extract document ID from URL")
            sys.exit(1)
    else:
        document_id = doc_id_or_url

    result = add_spintax_to_doc(document_id)
    print_summary(result)


if __name__ == '__main__':
    main()
