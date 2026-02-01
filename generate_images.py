"""Generate AI images for social media posts"""

import os
import json
import argparse
from typing import Dict, List
from datetime import datetime
from openai import OpenAI
import requests

# Initialize clients
# OpenRouter for GPT-4o (content analysis and prompts)
openrouter_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv('OPENROUTER_API_KEY')
)
# OpenAI for DALL-E 3 (image generation)
openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

def should_have_image(post: Dict, format_type: str) -> bool:
    """
    Determine if a post should have an accompanying image.

    Target: 30-40% of posts should have images

    Priority formats for images:
    - LinkedIn: Framework posts, data insights
    - X: Threads (first tweet), data bombs, personal stories

    Args:
        post: Post content dict
        format_type: Type of post (framework, thread, data_bomb, etc.)

    Returns:
        Boolean indicating if image is recommended
    """

    image_priority_formats = [
        'framework',
        'data_insight',
        'data_bomb',
        'thread',
        'lessons_learned'
    ]

    # Check if format is in priority list
    if any(fmt in format_type.lower() for fmt in image_priority_formats):
        return True

    # Check for keywords in post content that suggest visual storytelling
    post_text = post.get('post', '') if isinstance(post, dict) else str(post)
    visual_keywords = [
        'framework',
        'process',
        'step-by-step',
        'system',
        'funnel',
        'workflow',
        'diagram',
        'stats',
        'data',
        'results'
    ]

    if any(keyword in post_text.lower() for keyword in visual_keywords):
        return True

    return False


def generate_image_prompt(post_content: str, post_type: str, platform: str) -> str:
    """
    Create an image generation prompt based on post content.

    Args:
        post_content: The text content of the post
        post_type: Format type (thread, framework, data_bomb, etc.)
        platform: "linkedin" or "twitter"

    Returns:
        Image generation prompt string
    """

    # Use AI to analyze post and create image prompt
    analysis_prompt = f"""Analyze this social media post and create a prompt for generating an accompanying image.

POST CONTENT:
{post_content}

POST TYPE: {post_type}
PLATFORM: {platform}

The image should:
1. Visually represent the main concept
2. Be professional and B2B-appropriate
3. Use a minimalist, modern style
4. Include data visualization if the post mentions stats/numbers
5. Avoid stock photo aesthetics

Create an image generation prompt that describes:
- Main visual elements
- Style (professional, minimalist, modern, data-driven)
- Color scheme (suggest colors that work for cold email/B2B branding - blues, grays, whites, accent colors)
- Text overlay if needed (key stat or framework name)

Return ONLY the image generation prompt, no other text.
"""

    response = openrouter_client.chat.completions.create(
        model="openai/gpt-4o-mini",  # Use GPT-4o-mini for cost-effective prompt generation
        max_tokens=500,
        messages=[{"role": "user", "content": analysis_prompt}]
    )

    image_prompt = response.choices[0].message.content.strip()

    return image_prompt


def generate_image(prompt: str, post_id: str, output_dir: str) -> str:
    """
    Generate an image using DALL-E 3.

    Args:
        prompt: Image generation prompt
        post_id: Unique identifier for the post
        output_dir: Directory to save image

    Returns:
        Path to generated image file
    """

    os.makedirs(output_dir, exist_ok=True)

    print(f"  🎨 Generating image for {post_id}...")
    print(f"     Prompt: {prompt[:100]}...")

    try:
        # Generate image using DALL-E 3
        response = openai_client.images.generate(
            model="dall-e-3",
            prompt=prompt,
            size="1024x1024",  # Standard square format, works for both platforms
            quality="standard",  # "standard" or "hd" - standard is more cost-effective
            n=1
        )

        # Get the image URL from response
        image_url = response.data[0].url

        # Download the image
        image_response = requests.get(image_url)
        image_response.raise_for_status()

        # Save to file
        image_filename = f"{output_dir}/{post_id}.png"
        with open(image_filename, 'wb') as f:
            f.write(image_response.content)

        print(f"     ✅ Image saved to: {image_filename}")

        return image_filename

    except Exception as e:
        print(f"     ❌ Error generating image: {str(e)}")
        print(f"     Continuing without image for {post_id}")
        return None


def generate_images_for_posts(posts: List[Dict], output_dir: str = ".tmp/images") -> Dict[str, str]:
    """
    Generate images for all posts flagged as needing them.

    Args:
        posts: List of post dicts
        output_dir: Directory to save images

    Returns:
        Dict mapping post IDs to image file paths
    """

    print(f"\n{'='*60}")
    print(f"GENERATING IMAGES FOR POSTS")
    print(f"{'='*60}\n")

    images = {}
    posts_needing_images = []

    # Identify which posts need images
    for post in posts:
        post_id = post.get('id', f"post_{len(posts_needing_images)+1}")
        format_type = post.get('metadata', {}).get('format_type', post.get('format_type', ''))

        if post.get('needs_image') or should_have_image(post, format_type):
            posts_needing_images.append(post)

    target_image_count = int(len(posts) * 0.35)  # 35% target
    actual_image_count = len(posts_needing_images)

    print(f"📊 Total posts: {len(posts)}")
    print(f"🎯 Target images (35%): {target_image_count}")
    print(f"✅ Posts flagged for images: {actual_image_count}")
    print()

    # Generate images for flagged posts
    for i, post in enumerate(posts_needing_images, 1):
        post_id = post.get('id', f"post_{i}")
        post_content = post.get('post', '')
        format_type = post.get('metadata', {}).get('format_type', post.get('format_type', ''))
        platform = post.get('metadata', {}).get('platform', post.get('platform', ''))

        print(f"[{i}/{len(posts_needing_images)}] {post_id}")

        # Generate image prompt
        image_prompt = generate_image_prompt(post_content, format_type, platform)

        # Generate image
        image_path = generate_image(image_prompt, post_id, output_dir)

        # Only add to images dict if generation succeeded
        if image_path:
            images[post_id] = {
                'path': image_path,
                'prompt': image_prompt,
                'post_type': format_type
            }

        print()

    print(f"{'='*60}")
    print(f"IMAGE GENERATION COMPLETE")
    print(f"{'='*60}")
    print(f"Images generated: {len(images)}")
    print(f"Saved to: {output_dir}")
    print(f"{'='*60}\n")

    return images


def save_image_metadata(images: Dict, output_dir: str = ".tmp"):
    """Save image generation metadata"""

    os.makedirs(output_dir, exist_ok=True)

    metadata = {
        "images": images,
        "generated_at": datetime.now().isoformat(),
        "total_count": len(images)
    }

    filename = f"{output_dir}/image_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"💾 Image metadata saved to: {filename}\n")

    return filename


def main():
    parser = argparse.ArgumentParser(description='Generate AI images for social media posts')
    parser.add_argument('--posts-file', type=str, required=True,
                       help='JSON file containing posts to generate images for')
    parser.add_argument('--output-dir', type=str, default='.tmp/images',
                       help='Output directory for images (default: .tmp/images)')

    args = parser.parse_args()

    # Load posts
    with open(args.posts_file, 'r', encoding='utf-8') as f:
        posts = json.load(f)

    # Generate images
    images = generate_images_for_posts(posts, args.output_dir)

    # Save metadata
    save_image_metadata(images, os.path.dirname(args.output_dir))

    print("✅ Image generation complete!")
    print(f"   Generated {len(images)} images")
    print(f"   Saved to {args.output_dir}/")


if __name__ == "__main__":
    main()
