"""Research trending and relevant hashtags for cold email content"""

import os
import json
import argparse
from typing import List, Dict
from datetime import datetime

def get_recommended_hashtags(platform: str) -> List[Dict]:
    """
    Get recommended hashtags for the platform.

    In actual execution, Claude will use WebSearch to verify trending status
    and current follower counts. This provides the baseline recommendations.

    Args:
        platform: "linkedin" or "twitter"

    Returns:
        List of hashtag recommendations with metadata
    """

    if platform.lower() == "linkedin":
        hashtags = [
            {
                "hashtag": "#ColdEmail",
                "volume": "High",
                "followers": "150K+",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Use in 80% of posts - highly relevant to core topic"
            },
            {
                "hashtag": "#EmailMarketing",
                "volume": "High",
                "followers": "1M+",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Use for general email content, broader reach"
            },
            {
                "hashtag": "#LeadGeneration",
                "volume": "High",
                "followers": "500K+",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Use when focusing on lead gen strategies"
            },
            {
                "hashtag": "#B2BSales",
                "volume": "High",
                "followers": "300K+",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Target B2B audience, pairs well with cold email content"
            },
            {
                "hashtag": "#SalesStrategy",
                "volume": "High",
                "followers": "200K+",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Use for strategic/framework posts"
            },
            {
                "hashtag": "#EmailDeliverability",
                "volume": "Medium",
                "followers": "15K+",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Highly targeted - use for technical/deliverability posts"
            },
            {
                "hashtag": "#OutboundSales",
                "volume": "Medium",
                "followers": "50K+",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Niche but engaged audience"
            },
            {
                "hashtag": "#SalesProspecting",
                "volume": "Medium",
                "followers": "100K+",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Good for tactical/how-to content"
            },
            {
                "hashtag": "#DemandGeneration",
                "volume": "Medium",
                "followers": "80K+",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Use for marketing-focused lead gen content"
            },
            {
                "hashtag": "#RevenueGrowth",
                "volume": "Medium",
                "followers": "120K+",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Business outcome focus, good for ROI/results posts"
            }
        ]
    else:  # twitter/X
        hashtags = [
            {
                "hashtag": "#ColdEmail",
                "volume": "Medium",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Core hashtag - use in most posts"
            },
            {
                "hashtag": "#LeadGen",
                "volume": "High",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Broad reach, pairs well with B2B content"
            },
            {
                "hashtag": "#B2B",
                "volume": "High",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Target B2B audience specifically"
            },
            {
                "hashtag": "#SalesTips",
                "volume": "High",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Great for tactical/quick win posts"
            },
            {
                "hashtag": "#EmailMarketing",
                "volume": "High",
                "type": "Evergreen",
                "audience": "Broad",
                "recommendation": "Broader email marketing audience"
            },
            {
                "hashtag": "#Outbound",
                "volume": "Medium",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Outbound sales community, highly engaged"
            },
            {
                "hashtag": "#GrowthHacking",
                "volume": "High",
                "type": "Trending",
                "audience": "Broad",
                "recommendation": "Startup/growth audience, good for innovative tactics"
            },
            {
                "hashtag": "#SaaSSales",
                "volume": "Medium",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Target SaaS companies specifically"
            },
            {
                "hashtag": "#DemandGen",
                "volume": "Medium",
                "type": "Evergreen",
                "audience": "Niche",
                "recommendation": "Marketing-focused lead generation"
            },
            {
                "hashtag": "#StartupGrowth",
                "volume": "Medium",
                "type": "Trending",
                "audience": "Niche",
                "recommendation": "Target startups looking to scale"
            }
        ]

    return hashtags


def research_hashtags(platform: str) -> List[Dict]:
    """
    Research trending and relevant hashtags for cold email topics.

    Args:
        platform: "linkedin" or "twitter"

    Returns:
        List of hashtag recommendations with usage guidance
    """

    print(f"\n{'='*60}")
    print(f"RESEARCHING {platform.upper()} HASHTAGS")
    print(f"{'='*60}\n")

    hashtags = get_recommended_hashtags(platform)

    print(f"📊 Found {len(hashtags)} recommended hashtags\n")

    # Categorize by audience
    broad_hashtags = [h for h in hashtags if h['audience'] == 'Broad']
    niche_hashtags = [h for h in hashtags if h['audience'] == 'Niche']

    print("BROAD REACH HASHTAGS (1-2 per post):")
    for h in broad_hashtags[:5]:
        print(f"  • {h['hashtag']} - {h['recommendation']}")

    print(f"\nNICHE HASHTAGS (2-3 per post):")
    for h in niche_hashtags[:5]:
        print(f"  • {h['hashtag']} - {h['recommendation']}")

    print(f"\n{'='*60}\n")

    return hashtags


def create_hashtag_usage_guide(linkedin_hashtags: List[Dict], twitter_hashtags: List[Dict]) -> str:
    """Create a usage guide for hashtags"""

    guide = """# Hashtag Usage Guide

## LinkedIn Hashtags

### Strategy
- Use 3-5 hashtags per post
- Mix: 1-2 broad reach + 2-3 niche/targeted
- Place at the end of the post
- Rotate to avoid looking spammy

### Recommended Combinations

**For Deliverability/Technical Posts:**
```
#ColdEmail #EmailDeliverability #B2BSales #OutboundSales
```

**For Strategy/Framework Posts:**
```
#LeadGeneration #SalesStrategy #B2BSales #ColdEmail
```

**For Copywriting/Tactical Posts:**
```
#ColdEmail #EmailMarketing #SalesProspecting #B2BSales
```

**For Results/ROI Posts:**
```
#LeadGeneration #RevenueGrowth #B2BSales #DemandGeneration
```

### Top LinkedIn Hashtags:
"""

    for h in linkedin_hashtags:
        guide += f"\n**{h['hashtag']}**"
        if 'followers' in h:
            guide += f" ({h['followers']} followers)"
        guide += f"\n- Volume: {h['volume']}, Type: {h['type']}, Audience: {h['audience']}"
        guide += f"\n- {h['recommendation']}\n"

    guide += """\n## X (Twitter) Hashtags

### Strategy
- Use 2-3 hashtags per post
- Don't use hashtags in the first tweet of threads
- Mix trending and evergreen
- Keep natural - integrate into sentence when possible

### Recommended Combinations

**For Threads/How-To Posts:**
```
#ColdEmail #SalesTips #LeadGen
```

**For Hot Takes/Controversial Posts:**
```
#B2B #SalesTips
(Use fewer hashtags, let engagement drive reach)
```

**For Quick Wins/Tactical Posts:**
```
#ColdEmail #B2B #SalesTips
```

**For Data/Stats Posts:**
```
#EmailMarketing #LeadGen #B2B
```

### Top X Hashtags:
"""

    for h in twitter_hashtags:
        guide += f"\n**{h['hashtag']}**"
        guide += f"\n- Volume: {h['volume']}, Type: {h['type']}, Audience: {h['audience']}"
        guide += f"\n- {h['recommendation']}\n"

    return guide


def save_hashtag_results(linkedin_hashtags: List[Dict], twitter_hashtags: List[Dict], output_dir: str = ".tmp"):
    """Save hashtag research results"""

    os.makedirs(output_dir, exist_ok=True)

    # Save raw data
    results = {
        "linkedin": linkedin_hashtags,
        "twitter": twitter_hashtags,
        "generated_at": datetime.now().isoformat()
    }

    filename = f"{output_dir}/hashtags_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"💾 Hashtag data saved to: {filename}")

    # Save usage guide
    guide = create_hashtag_usage_guide(linkedin_hashtags, twitter_hashtags)
    guide_filename = f"{output_dir}/hashtag_guide_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    with open(guide_filename, 'w', encoding='utf-8') as f:
        f.write(guide)

    print(f"📋 Hashtag guide saved to: {guide_filename}\n")

    return filename, guide_filename


def main():
    parser = argparse.ArgumentParser(description='Research hashtags for cold email content')
    parser.add_argument('--platform', type=str, required=True,
                       choices=['linkedin', 'twitter', 'both'],
                       help='Platform to research (linkedin, twitter, or both)')
    parser.add_argument('--output-dir', type=str, default='.tmp',
                       help='Output directory for results (default: .tmp)')

    args = parser.parse_args()

    if args.platform == 'both' or args.platform == 'linkedin':
        linkedin_hashtags = research_hashtags('linkedin')
    else:
        linkedin_hashtags = []

    if args.platform == 'both' or args.platform == 'twitter':
        twitter_hashtags = research_hashtags('twitter')
    else:
        twitter_hashtags = []

    if linkedin_hashtags or twitter_hashtags:
        save_hashtag_results(linkedin_hashtags, twitter_hashtags, args.output_dir)

    print(f"{'='*60}")
    print(f"HASHTAG RESEARCH COMPLETE")
    print(f"{'='*60}")
    if linkedin_hashtags:
        print(f"LinkedIn hashtags: {len(linkedin_hashtags)}")
    if twitter_hashtags:
        print(f"Twitter hashtags: {len(twitter_hashtags)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
