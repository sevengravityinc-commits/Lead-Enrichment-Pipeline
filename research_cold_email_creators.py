"""Research top-performing cold email content from LinkedIn and X (Twitter)"""

import os
import json
import argparse
from typing import List, Dict
from datetime import datetime
from openai import OpenAI

# Initialize OpenRouter client
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv('OPENROUTER_API_KEY')
)

def search_web_for_posts(platform: str, query: str) -> List[Dict]:
    """
    Use web search to find high-performing posts.

    NOTE: This is a placeholder that would use Claude's WebSearch functionality.
    In the actual skill execution, Claude will use its WebSearch tool directly.

    Returns list of search results with URLs and snippets.
    """
    # This function will be called by Claude using its WebSearch tool
    # when the skill is invoked
    return []


def analyze_post_performance(post_content: str, engagement_metrics: Dict) -> Dict:
    """
    Use AI to analyze why a post performed well.

    Args:
        post_content: The full text of the post
        engagement_metrics: Dict with likes, comments, shares, etc.

    Returns:
        Dict with analysis and key takeaways
    """

    prompt = f"""Analyze this social media post about cold email/lead generation:

POST CONTENT:
{post_content}

ENGAGEMENT METRICS:
{json.dumps(engagement_metrics, indent=2)}

Provide analysis in this format:

1. WHY IT WORKED (2-3 sentences):
   - What made this post engaging?
   - What psychological triggers did it use?
   - What structure or format contributed to success?

2. KEY TAKEAWAYS (2-3 bullet points):
   - Specific lessons to apply to our content
   - Hook/structure patterns to replicate
   - Topics or angles that resonated

3. TOPIC CLASSIFICATION:
   Choose one: Email Copywriting / Email Deliverability / Campaign Strategy / Lead Generation

Return as JSON:
{{
    "why_it_worked": "...",
    "key_takeaways": ["...", "...", "..."],
    "topic": "...",
    "hook_pattern": "...",
    "content_type": "..."
}}
"""

    response = client.chat.completions.create(
        model="openai/gpt-4o-mini",  # Use GPT-4o-mini for cost-effective analysis
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )

    content = response.choices[0].message.content.strip()

    # Extract JSON from response
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0].strip()
    elif "```" in content:
        content = content.split("```")[1].split("```")[0].strip()

    try:
        analysis = json.loads(content)
        return analysis
    except json.JSONDecodeError:
        # Fallback if JSON parsing fails
        return {
            "why_it_worked": "Engaging content with clear value proposition",
            "key_takeaways": ["Use strong hooks", "Provide actionable insights", "Show vulnerability"],
            "topic": "General",
            "hook_pattern": "Unknown",
            "content_type": "Unknown"
        }


def research_cold_email_content(platform: str, days_back: int = 30) -> List[Dict]:
    """
    Research top-performing cold email content on specified platform.

    Args:
        platform: "linkedin" or "twitter"
        days_back: How many days to look back (default 30)

    Returns:
        List of dicts with post analysis
    """

    print(f"\n{'='*60}")
    print(f"RESEARCHING {platform.upper()} CONTENT")
    print(f"{'='*60}\n")

    # Search queries based on platform
    if platform.lower() == "linkedin":
        search_queries = [
            "cold email deliverability site:linkedin.com",
            "email copywriting B2B site:linkedin.com",
            "lead generation cold outreach site:linkedin.com"
        ]
    else:  # twitter/X
        search_queries = [
            "#ColdEmail high engagement",
            "#EmailDeliverability tips",
            "#LeadGen cold outreach"
        ]

    all_results = []

    print("🔍 Search queries:")
    for query in search_queries:
        print(f"  - {query}")

    print("\n📝 NOTE: This script provides a framework for research.")
    print("   When run as a skill, Claude will use WebSearch tool to find actual posts.")
    print("   For now, returning example structure...\n")

    # Example structure - in actual execution, Claude will use WebSearch
    # and populate this with real data
    example_results = [
        {
            "creator": "Example Creator 1",
            "topic": "Email Deliverability",
            "post_text": "[Post content would be extracted from web search]",
            "engagement": {
                "likes": 1200,
                "comments": 85,
                "shares": 45
            },
            "url": "https://example.com/post1",
            "platform": platform,
            "why_it_worked": "Strong hook with surprising statistic, provided actionable framework",
            "key_takeaways": [
                "Lead with data/stats to grab attention",
                "Provide step-by-step framework",
                "End with engagement question"
            ],
            "hook_pattern": "Surprising statistic",
            "content_type": "Framework post"
        }
    ]

    print(f"✅ Found {len(example_results)} high-performing posts")
    print(f"   (In actual execution, this would be 10-15 real posts)\n")

    return example_results


def save_research_results(results: List[Dict], platform: str, output_dir: str = ".tmp"):
    """Save research results to JSON file"""

    os.makedirs(output_dir, exist_ok=True)

    filename = f"{output_dir}/research_{platform}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"💾 Research results saved to: {filename}\n")

    return filename


def main():
    parser = argparse.ArgumentParser(description='Research top cold email content')
    parser.add_argument('--platform', type=str, required=True,
                       choices=['linkedin', 'twitter'],
                       help='Platform to research (linkedin or twitter)')
    parser.add_argument('--days', type=int, default=30,
                       help='Days to look back (default: 30)')
    parser.add_argument('--output-dir', type=str, default='.tmp',
                       help='Output directory for results (default: .tmp)')

    args = parser.parse_args()

    # Research content
    results = research_cold_email_content(args.platform, args.days)

    # Save results
    save_research_results(results, args.platform, args.output_dir)

    print(f"{'='*60}")
    print(f"RESEARCH COMPLETE")
    print(f"{'='*60}")
    print(f"Platform: {args.platform}")
    print(f"Posts found: {len(results)}")
    print(f"Topics covered: {len(set(r['topic'] for r in results))}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
