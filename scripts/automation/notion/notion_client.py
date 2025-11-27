from datetime import date
from typing import Optional, Any

import requests
from automation.config.settings import NotionSettings

# --- Load env once, from project root ---------------------------------

NOTION = NotionSettings()

NOTION_TOKEN = NOTION.token
NOTION_RELEASE_DB_ID: str | Any = NOTION.release_db_id

from .notion_schema import (
    build_release_properties,
    build_response_blocks,
)

# Optional debug – run once to confirm
print("DEBUG token present:", bool(NOTION_TOKEN))
print("DEBUG db id:", NOTION_RELEASE_DB_ID)

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


def create_release_page(
    *,
    name: str,
    summary: str,
    release_date: date,
    branch: str,
    full_summary: str,
    social_post: Optional[str] = None,
    dev_post: Optional[str] = None,
):
    """
    - Create a release row in the Releases DB
    - Write social post + JSON into the page body for that row
    """
    
    if release_date is None:
        release_date = date.today()

    # 1) Create the release row/page in the DB
    properties = build_release_properties(
        name=name,
        summary=summary,
        release_date=release_date,
        branch=branch,
    )
    

    create_payload = {
        "parent": {"database_id": NOTION_RELEASE_DB_ID},
        "properties": properties,
    }

    page_resp = requests.post(
        "https://api.notion.com/v1/pages",
        headers=HEADERS,
        json=create_payload,
    )
    
    page_resp.raise_for_status()
    page = page_resp.json()
    page_id = page["id"]
    
    blocks = build_response_blocks(
        full_summary=full_summary or "",
        social_post=social_post or "",
        dev_post=dev_post or "",
    )

    content_resp = requests.patch(
        f"https://api.notion.com/v1/blocks/{page_id}/children",
        headers=HEADERS,
        json={"children": blocks},
    )

    
    page_resp.raise_for_status()


    print("DEBUG status:", page_resp.status_code)
    print("DEBUG body:", page_resp.text)    
    return page
