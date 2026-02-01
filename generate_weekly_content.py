"""Main orchestrator for generating weekly social media content"""

import os
import json
import argparse
from typing import Dict, List, Tuple
from datetime import datetime
from openai import OpenAI
from pathlib import Path
import sys

# Fix Windows encoding for emojis
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Import our other modules
sys.path.append(os.path.dirname(__file__))
from generate_images import generate_images_for_posts

# Initialize OpenRouter client (OpenAI-compatible)
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv('OPENROUTER_API_KEY')
)

# Content generation prompts from directive
LINKEDIN_PROMPT_TEMPLATE = """Generate a LinkedIn post for a cold email agency owner.

POSITIONING: High-volume campaigns + personalization at scale + deliverability expert
TOPIC: {topic}
FORMAT: {format_type}
AUDIENCE: B2B business owners/executives seeking lead generation solutions

CRITICAL CONSTRAINT:
- NO fabricated case studies or client results
- User doesn't have real client data yet
- Focus on frameworks, hypothetical scenarios, lessons from experience, industry insights

RESEARCH INSPIRATION:
{research_example}

YOUR UNIQUE ANGLE:
{unique_angle}

REQUIREMENTS:
1. Hook: First 1-2 lines must grab attention (controversial or surprising)
2. Vulnerability: Share a personal mistake, struggle, or lesson
3. Expertise: Provide framework, data, or actionable insight
4. Value: NO bragging - focus on teaching
5. Engagement: End with a question
6. Length: 500-1000 words
7. Format: Line breaks every 2-3 lines
8. Hashtags: Include these at the end: {hashtags}
9. Tone: Professional thought leader who's not afraid to share failures
10. Ready to post: 99% complete, minimal editing needed

Generate the post now. Return ONLY the post content, no explanations."""

TWITTER_PROMPT_TEMPLATE = """Generate X (Twitter) content for a cold email agency owner.

POSITIONING: High-volume + personalization + deliverability expert
TOPIC: {topic}
FORMAT: {format_type}
AUDIENCE: B2B founders, sales leaders, marketers seeking leads

CRITICAL CONSTRAINT:
- NO fabricated metrics without clear disclaimer
- Focus on insights, lessons, and frameworks

RESEARCH INSPIRATION:
{research_example}

FORMAT-SPECIFIC REQUIREMENTS:
{format_instructions}

HASHTAGS TO USE: {hashtags}

REQUIREMENTS:
1. Engagement-optimized: Design for retweets and replies
2. Value-focused: Teach something useful
3. Authentic: Show vulnerability + expertise
4. Platform-appropriate: Follow X best practices
5. Ready to post: 99% complete

Generate the post now. Return ONLY the post content, no explanations."""

def select_topic_and_format(platform: str, day_index: int, research_data: List[Dict]) -> Tuple[str, str]:
    """
    Select topic and format for a post based on research and rotation strategy.

    Args:
        platform: "linkedin" or "twitter"
        day_index: Index for the day/post
        research_data: Research results to draw from

    Returns:
        Tuple of (topic, format_type)
    """

    if platform == "linkedin":
        formats = ["framework", "behind_the_scenes", "data_insight", "lessons_learned", "framework"]
        format_type = formats[day_index % len(formats)]

        topics = [
            "Email Deliverability",
            "Email Copywriting",
            "Campaign Strategy",
            "Lead Generation",
            "Personalization at Scale"
        ]
        topic = topics[day_index % len(topics)]

    else:  # twitter
        # 15 posts rotation: 3 threads, 4 hot takes, 4 quick wins, 2 data bombs, 2 stories
        format_rotation = [
            "thread", "hot_take", "quick_win",  # Day 1
            "data_bomb", "hot_take", "quick_win",  # Day 2
            "thread", "story", "quick_win",  # Day 3
            "data_bomb", "hot_take", "quick_win",  # Day 4
            "thread", "story", "hot_take"  # Day 5
        ]
        format_type = format_rotation[day_index]

        topics = [
            "Email Deliverability", "Copywriting Tactics", "Spam Triggers",
            "Subject Lines", "Personalization", "Email Warming",
            "SPF/DKIM/DMARC", "Reply Rates", "Campaign Mistakes",
            "A/B Testing", "Send Time Optimization", "List Building",
            "Follow-up Sequences", "CRM Integration", "Deliverability Tools"
        ]
        topic = topics[day_index % len(topics)]

    return topic, format_type


def get_format_instructions(format_type: str) -> str:
    """Get specific instructions for X/Twitter format"""

    instructions = {
        "thread": """- 5-10 tweets total
- Tweet 1: Hook that makes them want to read more
- Tweets 2-8: Step-by-step breakdown with examples
- Tweet 9: Summary + CTA
- Each tweet: 200-280 characters max
- Use line breaks for readability
- Numbers/bullets where helpful""",

        "hot_take": """- Single tweet: 200-280 characters
- Provocative but defensible statement
- Challenge conventional wisdom
- Design for engagement (make people think/argue)""",

        "quick_win": """- Single tweet: 150-200 characters
- One actionable tip
- Immediate value
- No fluff""",

        "data_bomb": """- 1-3 tweets
- Lead with surprising stat (from industry research)
- Explain why it matters
- Provide actionable insight""",

        "story": """- 2-4 tweets
- Share a mistake or struggle
- How you overcame it
- Lesson learned"""
    }

    return instructions.get(format_type, "")


def generate_linkedin_post(topic: str, format_type: str, hashtags: List[str], research_example: str = "") -> Dict:
    """Generate a single LinkedIn post"""

    hashtag_str = " ".join(hashtags[:5])  # Use up to 5 hashtags

    unique_angle = f"Provide your unique perspective on {topic} using the {format_type} format. Focus on lessons learned and frameworks, not client results."

    prompt = LINKEDIN_PROMPT_TEMPLATE.format(
        topic=topic,
        format_type=format_type,
        research_example=research_example if research_example else "N/A - create original content",
        unique_angle=unique_angle,
        hashtags=hashtag_str
    )

    print(f"  📝 Generating LinkedIn {format_type} post about {topic}...")

    response = client.chat.completions.create(
        model="openai/gpt-4o",  # Use GPT-4o via OpenRouter
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}]
    )

    post_content = response.choices[0].message.content.strip()

    return {
        'post': post_content,
        'topic': topic,
        'format_type': format_type,
        'platform': 'linkedin',
        'hashtags': hashtags[:5],
        'word_count': len(post_content.split())
    }


def generate_twitter_post(topic: str, format_type: str, hashtags: List[str], research_example: str = "") -> Dict:
    """Generate a single X/Twitter post"""

    hashtag_str = " ".join(hashtags[:3])  # Use up to 3 hashtags
    format_instructions = get_format_instructions(format_type)

    prompt = TWITTER_PROMPT_TEMPLATE.format(
        topic=topic,
        format_type=format_type,
        research_example=research_example if research_example else "N/A - create original content",
        format_instructions=format_instructions,
        hashtags=hashtag_str
    )

    print(f"  🐦 Generating X {format_type} post about {topic}...")

    response = client.chat.completions.create(
        model="openai/gpt-4o",  # Use GPT-4o via OpenRouter
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    )

    post_content = response.choices[0].message.content.strip()

    # Determine posting time based on format
    time_mapping = {
        "thread": "morning",
        "data_bomb": "morning",
        "quick_win": "afternoon",
        "hot_take": "afternoon",
        "story": "evening"
    }

    return {
        'post': post_content,
        'topic': topic,
        'format_type': format_type,
        'platform': 'twitter',
        'hashtags': hashtags[:3],
        'recommended_time': time_mapping.get(format_type, 'afternoon'),
        'char_count': len(post_content)
    }


def identify_context_needs(posts: List[Dict]) -> List[Dict]:
    """Identify posts that need user context input"""

    context_requests = []

    for post in posts:
        post_content = post['post'].lower()

        # Check for placeholders or context-needing phrases
        context_keywords = [
            'specific tool',
            'your workflow',
            'the tools you use',
            'your process',
            'your approach',
            'in my experience',
            'what works for me'
        ]

        needs_context = any(keyword in post_content for keyword in context_keywords)

        if needs_context:
            context_requests.append({
                'post_id': post.get('id', 'unknown'),
                'platform': post['platform'],
                'topic': post['topic'],
                'question': f"This post about {post['topic']} may need specific context. Please review and add personal details as needed."
            })

    return context_requests


def generate_weekly_content(week_number: int, output_dir: str = "output") -> Dict:
    """
    Generate full week of social media content.

    Args:
        week_number: Week number for organization
        output_dir: Base output directory

    Returns:
        Dict with paths to all generated files
    """

    print(f"\n{'='*70}")
    print(f"GENERATING WEEKLY SOCIAL MEDIA CONTENT - WEEK {week_number}")
    print(f"{'='*70}\n")

    # Create output directory
    week_dir = f"{output_dir}/social_content_week_{week_number}"
    os.makedirs(week_dir, exist_ok=True)
    os.makedirs(f"{week_dir}/images", exist_ok=True)

    # Load hashtag recommendations (from previous research)
    # In actual execution, these would come from research_hashtags.py output
    linkedin_hashtags = ["#ColdEmail", "#EmailMarketing", "#LeadGeneration", "#B2BSales", "#EmailDeliverability"]
    twitter_hashtags = ["#ColdEmail", "#LeadGen", "#B2B", "#SalesTips"]

    print("📊 CONTENT PLAN:")
    print(f"  • LinkedIn: 5 posts")
    print(f"  • X/Twitter: 15 posts (3 per day)")
    print(f"  • Total: 20 posts\n")

    # Generate LinkedIn posts (exactly 5)
    print("🔵 GENERATING LINKEDIN CONTENT...")
    print(f"{'='*70}\n")

    linkedin_posts = []
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

    for i, day in enumerate(days):
        topic, format_type = select_topic_and_format("linkedin", i, [])

        post = generate_linkedin_post(topic, format_type, linkedin_hashtags)
        post['id'] = f"linkedin_post_{i+1}"
        post['day'] = day
        post['needs_image'] = format_type in ['framework', 'data_insight']

        linkedin_posts.append(post)
        print(f"  ✅ {day}: {format_type} ({post['word_count']} words)\n")

    # Generate X/Twitter posts (exactly 15 = 3 per day)
    print(f"\n🔵 GENERATING X/TWITTER CONTENT...")
    print(f"{'='*70}\n")

    twitter_posts = []
    post_index = 0

    for day_index, day in enumerate(days):
        print(f"  📅 {day}:")

        for time_slot in range(3):  # 3 posts per day
            topic, format_type = select_topic_and_format("twitter", post_index, [])

            post = generate_twitter_post(topic, format_type, twitter_hashtags)
            post['id'] = f"twitter_post_{post_index+1}"
            post['day'] = day
            post['time_slot'] = time_slot + 1
            post['needs_image'] = format_type in ['thread', 'data_bomb']

            twitter_posts.append(post)
            print(f"    {post['recommended_time']}: {format_type} ({post['char_count']} chars)")

            post_index += 1

        print()

    all_posts = linkedin_posts + twitter_posts

    # Identify posts needing images
    posts_needing_images = [p for p in all_posts if p.get('needs_image', False)]
    print(f"🎨 Posts flagged for images: {len(posts_needing_images)} ({len(posts_needing_images)/len(all_posts)*100:.0f}%)\n")

    # Generate images for flagged posts
    if posts_needing_images:
        images_dir = f"{week_dir}/images"
        images = generate_images_for_posts(all_posts, images_dir)
        print(f"✅ Generated {len(images)} images\n")
    else:
        images = {}
        print("No images needed for this week\n")

    # Identify context needs
    context_requests = identify_context_needs(all_posts)
    print(f"❓ Posts needing user context: {len(context_requests)}\n")

    # Save all posts to JSON for further processing
    posts_file = f"{week_dir}/all_posts.json"
    with open(posts_file, 'w', encoding='utf-8') as f:
        json.dump({
            'linkedin': linkedin_posts,
            'twitter': twitter_posts,
            'metadata': {
                'week': week_number,
                'generated_at': datetime.now().isoformat(),
                'total_posts': len(all_posts),
                'posts_with_images': len(posts_needing_images),
                'images_generated': len(images),
                'context_requests': len(context_requests)
            }
        }, f, indent=2, ensure_ascii=False)

    print(f"💾 Posts saved to: {posts_file}\n")

    # Generate context requests file
    if context_requests:
        context_file = f"{week_dir}/CONTEXT_REQUESTS.md"
        with open(context_file, 'w', encoding='utf-8') as f:
            f.write("# Context Requests\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            f.write(f"The following {len(context_requests)} posts may benefit from additional context:\n\n")

            for i, req in enumerate(context_requests, 1):
                f.write(f"## {i}. {req['post_id']} ({req['platform']})\n")
                f.write(f"**Topic:** {req['topic']}\n\n")
                f.write(f"**Question:** {req['question']}\n\n")
                f.write("---\n\n")

        print(f"❓ Context requests saved to: {context_file}\n")

    # Summary
    print(f"{'='*70}")
    print(f"CONTENT GENERATION COMPLETE")
    print(f"{'='*70}")
    print(f"✅ Generated 5 LinkedIn posts")
    print(f"✅ Generated 15 X/Twitter posts")
    print(f"🎨 Generated {len(images)} AI images")
    print(f"❓ {len(context_requests)} context requests")
    print(f"📁 Output directory: {week_dir}/")
    print(f"{'='*70}\n")

    print("📋 NEXT STEPS:")
    print("  1. Run format_content_calendar.py to create organized calendar")
    print("  2. Review CONTEXT_REQUESTS.md and add personal context")
    print("  3. Review and schedule posts\n")

    return {
        'week_dir': week_dir,
        'posts_file': posts_file,
        'linkedin_count': len(linkedin_posts),
        'twitter_count': len(twitter_posts),
        'image_count': len(images),
        'context_count': len(context_requests)
    }


def main():
    parser = argparse.ArgumentParser(description='Generate weekly social media content')
    parser.add_argument('--week', type=int, default=1,
                       help='Week number (default: 1)')
    parser.add_argument('--output-dir', type=str, default='output',
                       help='Output directory (default: output)')

    args = parser.parse_args()

    # Generate content
    results = generate_weekly_content(args.week, args.output_dir)

    print("✅ Generation complete!")
    print(f"   Output: {results['week_dir']}/")


if __name__ == "__main__":
    main()
