"""
Helper to pull the social post text from a Notion release page and publish to Mastodon.

This intentionally keeps the existing Notion write flow untouched. Usage pattern:
    python -m automation.notion.notion_to_mastodon --page-id <notion_page_id>
        [--visibility unlisted] [--dry-run]

Env vars required:
    NOTION_TOKEN
    MASTODON_BASE_URL   (e.g. https://mastodon.social)
    MASTODON_ACCESS_TOKEN
"""

import argparse
from typing import List, Optional, Tuple

import requests
from automation.config.settings import NotionSettings, MastodonSettings


NOTION = NotionSettings()
MASTODON = MastodonSettings()

NOTION_TOKEN = NOTION.token
NOTION_RELEASE_DB_ID = NOTION.release_db_id

MASTODON_BASE_URL = MASTODON.base_url
MASTODON_ACCESS_TOKEN = MASTODON.access_token

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

MASTODON_HEADERS = {
    "Authorization": f"Bearer {MASTODON_ACCESS_TOKEN}",
}


class ConfigError(RuntimeError):
    pass


def require_env() -> None:
    missing: List[str] = []
    if not NOTION_TOKEN:
        missing.append("NOTION_TOKEN")
    if not NOTION_RELEASE_DB_ID:
        missing.append("NOTION_RELEASE_DB_ID")
    if not MASTODON_BASE_URL:
        missing.append("MASTODON_BASE_URL")
    if not MASTODON_ACCESS_TOKEN:
        missing.append("MASTODON_ACCESS_TOKEN")
    if missing:
        raise ConfigError(f"Missing required env vars: {', '.join(missing)}")


def fetch_social_post_from_notion(page_id: str) -> str:
    """Retrieve the social post text under the "Social media post" heading."""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    has_more = True
    next_cursor: Optional[str] = None
    blocks: List[dict] = []

    # This code needs to be fixed 
    while has_more:
        params = {"page_size": 100}
        if next_cursor:
            params["start_cursor"] = next_cursor
        resp = requests.get(url, headers=NOTION_HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        blocks.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")

    social_texts: List[str] = []
    capture_next = False

    for block in blocks:
        block_type = block.get("type")
        if block_type == "heading_3":
            rich_text = block[block_type].get("rich_text", [])
            heading_text = "".join(rt.get("plain_text", "") for rt in rich_text).strip()
            capture_next = heading_text.lower() == "social media post"
            continue

        if capture_next:
            rich_text = block.get(block_type, {}).get("rich_text", [])
            text_content = "".join(rt.get("plain_text", "") for rt in rich_text).strip()
            if text_content:
                social_texts.append(text_content)
            # Stop after first non-empty text block to avoid grabbing dev section
            if text_content:
                break

    if not social_texts:
        raise ValueError("Could not find social post content under the expected heading")

    return "\n".join(social_texts)


def fetch_latest_release_page() -> Tuple[str, str]:
    """Return (page_id, title) for the most recently edited release page."""
    url = f"https://api.notion.com/v1/databases/{NOTION_RELEASE_DB_ID}/query"
    payload = {
        "page_size": 1,
        "sorts": [
            {"timestamp": "last_edited_time", "direction": "descending"}
        ],
    }

    resp = requests.post(url, headers=NOTION_HEADERS, json=payload)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        raise ValueError("No release pages found in Notion database")

    page = results[0]
    page_id = page.get("id")

    # Attempt to extract the title from properties
    title_props = page.get("properties", {}).values()
    title_text = ""
    for prop in title_props:
        if prop.get("type") == "title":
            rich = prop.get("title", [])
            title_text = "".join(rt.get("plain_text", "") for rt in rich).strip()
            break

    return page_id, title_text


def post_to_mastodon(status: str, visibility: str = "public", dry_run: bool = False) -> dict:
    """Post a status to Mastodon. Returns the Mastodon API response JSON."""
    payload = {"status": status, "visibility": visibility}

    if dry_run:
        print("[dry-run] Would post to Mastodon:")
        print(status)
        return {"dry_run": True, "status": status, "visibility": visibility}

    resp = requests.post(
        f"{MASTODON_BASE_URL}/api/v1/statuses",
        data=payload,
        headers=MASTODON_HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Post a Notion social update to Mastodon")
    parser.add_argument("--page-id", help="Notion release page ID")
    parser.add_argument("--latest", action="store_true", help="Use the most recently edited release page")
    parser.add_argument("--visibility", default="public", choices=["public", "unlisted", "private", "direct"], help="Mastodon visibility")
    parser.add_argument("--dry-run", action="store_true", help="Skip network post; print payload")
    args = parser.parse_args()

    require_env()

    page_id = args.page_id
    page_title = ""

    if args.latest:
        page_id, page_title = fetch_latest_release_page()
        print(f"Using latest release page: {page_title or '[no title]'} ({page_id})")

    if not page_id:
        raise ValueError("Provide --page-id or --latest")

    social_post = fetch_social_post_from_notion(page_id)
    print(f"Fetched social post (len={len(social_post)}):\n{social_post}\n")

    response = post_to_mastodon(social_post, visibility=args.visibility, dry_run=args.dry_run)
    print("Mastodon response:", response)


if __name__ == "__main__":
    main()
