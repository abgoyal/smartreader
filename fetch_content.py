#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "httpx",
#     "readability-lxml",
#     "markdownify",
#     "beautifulsoup4",
#     "lxml",
# ]
# ///
"""
Fetch a URL and extract readable content as markdown with local images.

Usage:
    ./fetch_content.py <url> [options]

Options:
    --output, -o DIR    Output directory (default: ./fetched/<date>-<slug>)
    --no-images         Don't download images
    --timeout SECS      Request timeout (default: 30)

Example:
    ./fetch_content.py https://example.com/article --output ./my-article
"""

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from readability import Document


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    text = text.lower().strip()
    text = re.sub(r"\s+", "-", text)  # spaces to hyphens
    text = re.sub(r"[^\w\-]", "", text)  # remove non-word chars
    text = re.sub(r"-+", "-", text)  # collapse multiple hyphens
    text = text.strip("-")
    return text[:80] if text else "untitled"


def get_extension(url: str, content_type: str | None = None) -> str:
    """Determine file extension from URL or content-type."""
    # Try URL path first
    path = urlparse(url).path
    ext = Path(path).suffix.lower()
    if ext and len(ext) <= 5 and re.match(r"^\.[a-z0-9]+$", ext):
        return ext

    # Try content-type
    if content_type:
        ct = content_type.lower()
        if "jpeg" in ct or "jpg" in ct:
            return ".jpg"
        if "png" in ct:
            return ".png"
        if "gif" in ct:
            return ".gif"
        if "webp" in ct:
            return ".webp"
        if "svg" in ct:
            return ".svg"
        if "avif" in ct:
            return ".avif"

    return ".jpg"  # default


def sanitize_filename(name: str) -> str:
    """Sanitize a string for use as filename."""
    # URL decode
    try:
        from urllib.parse import unquote

        name = unquote(name)
    except Exception:
        pass

    name = re.sub(r"[^\w\-]", "-", name)
    name = re.sub(r"-+", "-", name)
    name = name.strip("-")
    if not name or len(name) < 2:
        name = "image"
    return name[:50]


def parse_srcset(srcset: str) -> list[tuple[str, int]]:
    """Parse srcset attribute and return list of (url, width) tuples."""
    results = []
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        pieces = part.split()
        if not pieces:
            continue
        url = pieces[0]
        width = 0
        if len(pieces) > 1:
            # Parse width descriptor like "800w" or "2x"
            descriptor = pieces[1].lower()
            if descriptor.endswith("w"):
                try:
                    width = int(descriptor[:-1])
                except ValueError:
                    pass
            elif descriptor.endswith("x"):
                try:
                    width = int(float(descriptor[:-1]) * 100)  # rough estimate
                except ValueError:
                    pass
        results.append((url, width))
    return results


def get_best_image_url(img_tag, base_url: str) -> str | None:
    """
    Get the best image URL from an img tag.
    Handles: src, srcset, data-src, data-lazy-src, data-original
    """
    # Check for lazy-loading attributes first
    lazy_attrs = ["data-src", "data-lazy-src", "data-original", "data-lazy"]
    for attr in lazy_attrs:
        val = img_tag.get(attr)
        if val and not val.startswith("data:"):
            return urljoin(base_url, val)

    # Check srcset for highest resolution
    srcset = img_tag.get("srcset")
    if srcset:
        parsed = parse_srcset(srcset)
        if parsed:
            # Sort by width descending, pick largest
            parsed.sort(key=lambda x: x[1], reverse=True)
            best_url = parsed[0][0]
            if not best_url.startswith("data:"):
                return urljoin(base_url, best_url)

    # Fall back to src
    src = img_tag.get("src")
    if src and not src.startswith("data:"):
        return urljoin(base_url, src)

    return None


def download_image(client: httpx.Client, img_url: str, output_dir: Path) -> str | None:
    """Download image and return local filename."""
    try:
        # Skip data URLs
        if img_url.startswith("data:"):
            return None

        # Parse URL for filename base
        parsed = urlparse(img_url)
        basename = Path(parsed.path).stem
        basename = sanitize_filename(basename)

        # Generate hash for uniqueness
        url_hash = hashlib.md5(img_url.encode()).hexdigest()[:8]

        # Fetch image
        resp = client.get(img_url, follow_redirects=True)
        resp.raise_for_status()

        ext = get_extension(img_url, resp.headers.get("content-type"))
        filename = f"{basename}-{url_hash}{ext}"
        filepath = output_dir / filename

        # Don't re-download
        if not filepath.exists():
            filepath.write_bytes(resp.content)

        return filename

    except Exception as e:
        print(f"  Failed to download {img_url}: {e}", file=sys.stderr)
        return None


def preprocess_html(soup: BeautifulSoup, base_url: str) -> None:
    """
    Pre-process HTML before Readability extraction.
    Mirrors prettyblog's add-link.js preprocessing.
    """
    # 1. Unwrap <picture> elements - extract best source or img
    for picture in soup.find_all("picture"):
        # Try to find the best source
        best_url = None
        best_width = 0

        # Check <source> elements
        for source in picture.find_all("source"):
            srcset = source.get("srcset")
            if srcset:
                parsed = parse_srcset(srcset)
                for url, width in parsed:
                    if width > best_width:
                        best_width = width
                        best_url = url

        # Check the img inside
        img = picture.find("img")
        if img:
            img_url = get_best_image_url(img, base_url)
            if img_url:
                # Update img src to best found URL
                if best_url and best_width > 0:
                    img["src"] = urljoin(base_url, best_url)
                else:
                    img["src"] = img_url
                # Clear srcset to avoid confusion
                if img.get("srcset"):
                    del img["srcset"]
                picture.replace_with(img)
            else:
                picture.decompose()
        else:
            picture.decompose()

    # 2. Unwrap <figure> from wrapper divs (Readability sometimes strips these)
    for figure in soup.find_all("figure"):
        parent = figure.parent
        if parent and parent.name == "div":
            # Check if div only contains this figure
            children = [c for c in parent.children if getattr(c, "name", None)]
            if len(children) == 1 and children[0] == figure:
                parent.replace_with(figure)

    # 3. Handle lazy-loaded images - promote data-src to src
    for img in soup.find_all("img"):
        best_url = get_best_image_url(img, base_url)
        if best_url:
            img["src"] = best_url
            # Clean up lazy attributes
            for attr in [
                "data-src",
                "data-lazy-src",
                "data-original",
                "data-lazy",
                "srcset",
            ]:
                if img.get(attr):
                    del img[attr]

    # 4. Fix relative URLs for links
    for a in soup.find_all("a"):
        href = a.get("href")
        if href and not href.startswith(
            ("http://", "https://", "mailto:", "#", "javascript:")
        ):
            a["href"] = urljoin(base_url, href)


def fetch_and_extract(
    url: str,
    output_dir: Path | None = None,
    download_images: bool = True,
    timeout: int = 30,
) -> dict:
    """
    Fetch URL and extract readable content.

    Returns dict with: title, excerpt, markdown, output_dir
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        print(f"Fetching: {url}")
        resp = client.get(url, headers=headers)
        resp.raise_for_status()

        html = resp.text

        # Pre-process HTML before Readability
        pre_soup = BeautifulSoup(html, "lxml")
        preprocess_html(pre_soup, url)
        html = str(pre_soup)

        # Use readability to extract article
        doc = Document(html)
        title = doc.title() or "Untitled"
        summary = doc.summary()

        # Parse the extracted content for further processing
        soup = BeautifulSoup(summary, "lxml")

        # Get excerpt from first paragraph
        first_p = soup.find("p")
        excerpt = ""
        if first_p:
            excerpt = first_p.get_text(strip=True)[:200]

        # Determine output directory
        if output_dir is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            slug = slugify(title)
            folder_name = f"{date_str}-{slug}"
            output_dir = Path("fetched") / folder_name

        output_dir.mkdir(parents=True, exist_ok=True)

        # Download images and update references
        image_count = 0
        if download_images:
            print("Downloading images...")
            for img in soup.find_all("img"):
                img_url = get_best_image_url(img, url)
                if not img_url:
                    continue

                # Download
                local_name = download_image(client, img_url, output_dir)
                if local_name:
                    print(f"  Downloaded: {local_name}")
                    img["src"] = local_name
                    image_count += 1

        # Convert to markdown
        markdown_body = md(
            str(soup),
            heading_style="ATX",
            code_language_callback=lambda _: "",
        )

        # Clean up markdown
        # Fix linked images being split across multiple lines
        markdown_body = re.sub(
            r"\[\s*(!\[.*?\]\(.*?\))\s*\]\((.*?)\)",
            r"[\1](\2)",
            markdown_body,
        )

        # Remove excessive blank lines
        markdown_body = re.sub(r"\n{3,}", "\n\n", markdown_body)

        # Strip leading/trailing whitespace
        markdown_body = markdown_body.strip()

        # Create frontmatter
        now = datetime.now(timezone.utc).isoformat()
        safe_title = title.replace('"', '\\"')
        safe_excerpt = excerpt.replace('"', '\\"')

        content = f'''---
title: "{safe_title}"
date: {now}
source_url: "{url}"
excerpt: "{safe_excerpt}"
---

[Original Link]({url})

---

{markdown_body}
'''

        # Write output
        output_file = output_dir / "index.md"
        output_file.write_text(content)

        return {
            "title": title,
            "excerpt": excerpt,
            "markdown": markdown_body,
            "output_dir": output_dir,
            "output_file": output_file,
            "image_count": image_count,
        }


def main():
    parser = argparse.ArgumentParser(
        description="Fetch URL and extract readable content as markdown"
    )
    parser.add_argument("url", help="URL to fetch")
    parser.add_argument(
        "--output", "-o", type=Path, help="Output directory", default=None
    )
    parser.add_argument(
        "--no-images", action="store_true", help="Don't download images"
    )
    parser.add_argument(
        "--timeout", type=int, default=30, help="Request timeout in seconds"
    )

    args = parser.parse_args()

    try:
        result = fetch_and_extract(
            url=args.url,
            output_dir=args.output,
            download_images=not args.no_images,
            timeout=args.timeout,
        )

        print(f"\nSaved: {result['output_file']}")
        print(f"Title: {result['title']}")
        print(f"Size:  {len(result['markdown'])} chars")
        if result["image_count"]:
            print(f"Images: {result['image_count']}")

    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e.response.status_code}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
