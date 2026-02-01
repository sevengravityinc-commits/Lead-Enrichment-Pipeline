"""Format generated content into organized calendar and individual post files"""

import os
import json
import argparse
from datetime import datetime
from typing import Dict, List
import sys

# Fix Windows encoding for emojis
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def create_content_calendar(linkedin_posts: List[Dict], twitter_posts: List[Dict], output_file: str):
    """Create master calendar with all posts organized by day"""

    print(f"📅 Creating content calendar...")

    calendar = f"""# Social Media Content Calendar - Week {linkedin_posts[0].get('week', 1) if linkedin_posts else 1}

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}

## Overview
- **LinkedIn:** {len(linkedin_posts)} posts
- **X (Twitter):** {len(twitter_posts)} posts
- **Total content pieces:** {len(linkedin_posts) + len(twitter_posts)}
- **Posting schedule:** 5 days (Monday-Friday)

---

"""

    # Organize by day
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

    for day in days:
        calendar += f"## {day}\n\n"

        # LinkedIn post for this day
        day_linkedin = [p for p in linkedin_posts if p.get('day') == day]
        if day_linkedin:
            post = day_linkedin[0]
            calendar += f"### LinkedIn Post\n"
            calendar += f"**Topic:** {post['topic']}\n"
            calendar += f"**Format:** {post['format_type'].replace('_', ' ').title()}\n"
            calendar += f"**Word Count:** {post.get('word_count', 'N/A')}\n"
            calendar += f"**Hashtags:** {' '.join(post.get('hashtags', []))}\n"

            if post.get('needs_image'):
                calendar += f"**Image:** ✅ AI-generated image recommended\n"

            calendar += f"\n**Post Content:**\n\n"
            calendar += f"```\n{post['post']}\n```\n\n"
            calendar += f"---\n\n"

        # Twitter posts for this day
        day_twitter = [p for p in twitter_posts if p.get('day') == day]
        day_twitter.sort(key=lambda x: x.get('time_slot', 0))

        if day_twitter:
            calendar += f"### X (Twitter) Posts ({len(day_twitter)} posts)\n\n"

            for i, post in enumerate(day_twitter, 1):
                time_emoji = {"morning": "🌅", "afternoon": "☀️", "evening": "🌙"}.get(post['recommended_time'], "⏰")

                calendar += f"#### Post {i} - {time_emoji} {post['recommended_time'].title()}\n"
                calendar += f"**Topic:** {post['topic']}\n"
                calendar += f"**Format:** {post['format_type'].replace('_', ' ').title()}\n"
                calendar += f"**Character Count:** {post.get('char_count', 'N/A')}\n"
                calendar += f"**Hashtags:** {' '.join(post.get('hashtags', []))}\n"

                if post.get('needs_image'):
                    calendar += f"**Image:** ✅ AI-generated image recommended\n"

                calendar += f"\n**Post Content:**\n\n"
                calendar += f"```\n{post['post']}\n```\n\n"

            calendar += f"---\n\n"

        calendar += "\n"

    # Save calendar
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(calendar)

    print(f"  ✅ Calendar saved to: {output_file}\n")


def create_linkedin_posts_file(posts: List[Dict], output_file: str):
    """Create file with all LinkedIn posts ready to copy"""

    print(f"📄 Creating LinkedIn posts file...")

    content = f"""# LinkedIn Posts - Ready to Copy

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}

Total posts: {len(posts)}

---

"""

    for i, post in enumerate(posts, 1):
        content += f"## Post {i} - {post.get('day', 'N/A')} ({post['format_type'].replace('_', ' ').title()})\n\n"
        content += f"**Topic:** {post['topic']}\n"
        content += f"**Word Count:** {post.get('word_count', 'N/A')}\n\n"

        if post.get('needs_image'):
            content += f"**📸 Image recommended** - See images/ folder for `{post.get('id', 'unknown')}.png`\n\n"

        content += f"### Copy below:\n\n"
        content += f"{post['post']}\n\n"
        content += f"---\n\n"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"  ✅ LinkedIn posts saved to: {output_file}\n")


def create_twitter_posts_file(posts: List[Dict], output_file: str):
    """Create file with all X/Twitter posts ready to copy"""

    print(f"📄 Creating X/Twitter posts file...")

    content = f"""# X (Twitter) Posts - Ready to Copy

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}

Total posts: {len(posts)}
Daily frequency: 3 posts per day

---

"""

    # Group by day
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

    for day in days:
        day_posts = [p for p in posts if p.get('day') == day]
        day_posts.sort(key=lambda x: x.get('time_slot', 0))

        if day_posts:
            content += f"## {day} ({len(day_posts)} posts)\n\n"

            for i, post in enumerate(day_posts, 1):
                time_emoji = {"morning": "🌅", "afternoon": "☀️", "evening": "🌙"}.get(post['recommended_time'], "⏰")

                content += f"### {time_emoji} {post['recommended_time'].title()} - {post['format_type'].replace('_', ' ').title()}\n\n"
                content += f"**Topic:** {post['topic']}\n"
                content += f"**Character Count:** {post.get('char_count', 'N/A')}\n\n"

                if post.get('needs_image'):
                    content += f"**📸 Image recommended** - See images/ folder for `{post.get('id', 'unknown')}.png`\n\n"

                content += f"### Copy below:\n\n"
                content += f"{post['post']}\n\n"
                content += f"---\n\n"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"  ✅ X/Twitter posts saved to: {output_file}\n")


def create_hashtag_summary(linkedin_posts: List[Dict], twitter_posts: List[Dict], output_file: str):
    """Create hashtag usage summary"""

    print(f"📄 Creating hashtag summary...")

    content = f"""# Hashtag Summary

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}

## LinkedIn Hashtags Used

"""

    # Collect all unique LinkedIn hashtags
    linkedin_hashtags = set()
    for post in linkedin_posts:
        linkedin_hashtags.update(post.get('hashtags', []))

    for tag in sorted(linkedin_hashtags):
        content += f"- {tag}\n"

    content += f"\n## X (Twitter) Hashtags Used\n\n"

    # Collect all unique Twitter hashtags
    twitter_hashtags = set()
    for post in twitter_posts:
        twitter_hashtags.update(post.get('hashtags', []))

    for tag in sorted(twitter_hashtags):
        content += f"- {tag}\n"

    content += f"""

## Usage Guidelines

### LinkedIn (3-5 hashtags per post)
- Mix 1-2 broad reach hashtags with 2-3 niche/targeted ones
- Place at the end of the post
- Rotate to avoid looking spammy

### X/Twitter (2-3 hashtags per post)
- Mix trending and evergreen
- Don't use in first tweet of threads
- Keep natural - integrate into sentence when possible
"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"  ✅ Hashtag summary saved to: {output_file}\n")


def format_all_content(posts_file: str, output_dir: str = None):
    """
    Format all content from generated posts JSON file.

    Args:
        posts_file: Path to all_posts.json file
        output_dir: Output directory (defaults to same dir as posts_file)
    """

    print(f"\n{'='*70}")
    print(f"FORMATTING CONTENT CALENDAR")
    print(f"{'='*70}\n")

    # Load posts
    with open(posts_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    linkedin_posts = data.get('linkedin', [])
    twitter_posts = data.get('twitter', [])

    if output_dir is None:
        output_dir = os.path.dirname(posts_file)

    print(f"📊 Loaded:")
    print(f"  • {len(linkedin_posts)} LinkedIn posts")
    print(f"  • {len(twitter_posts)} X/Twitter posts\n")

    # Create all output files
    create_content_calendar(
        linkedin_posts,
        twitter_posts,
        f"{output_dir}/CONTENT_CALENDAR.md"
    )

    create_linkedin_posts_file(
        linkedin_posts,
        f"{output_dir}/LINKEDIN_POSTS.md"
    )

    create_twitter_posts_file(
        twitter_posts,
        f"{output_dir}/X_POSTS.md"
    )

    create_hashtag_summary(
        linkedin_posts,
        twitter_posts,
        f"{output_dir}/HASHTAGS.md"
    )

    print(f"{'='*70}")
    print(f"FORMATTING COMPLETE")
    print(f"{'='*70}")
    print(f"📁 Output directory: {output_dir}/")
    print(f"📄 Files created:")
    print(f"  • CONTENT_CALENDAR.md")
    print(f"  • LINKEDIN_POSTS.md")
    print(f"  • X_POSTS.md")
    print(f"  • HASHTAGS.md")
    print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(description='Format content calendar from generated posts')
    parser.add_argument('--posts-file', type=str, required=True,
                       help='Path to all_posts.json file')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Output directory (defaults to same dir as posts file)')

    args = parser.parse_args()

    format_all_content(args.posts_file, args.output_dir)

    print("✅ Content calendar formatting complete!")


if __name__ == "__main__":
    main()
