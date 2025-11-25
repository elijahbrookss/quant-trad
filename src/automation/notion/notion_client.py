import os
import json
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
# --- Load env once, from project root ---------------------------------

# 1) Load the same secrets.env your test file uses
#  (absolute path so imports don't break it)
#load_dotenv(dotenv_path="/home/jorge/projects/quant-trad/secrets.env")
# parents[0] = notion
# parents[1] = automation
# parents[2] = src
# parents[3] = quant-trad (repo root)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = PROJECT_ROOT / "secrets.env"
load_dotenv(PROJECT_ROOT / "secrets.env")

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_RELEASE_DB_ID = os.getenv("NOTION_RELEASE_DB_ID")

if NOTION_TOKEN is None:
    raise ValueError("Missing NOTION_TOKEN env var")
if NOTION_RELEASE_DB_ID is None:
    raise ValueError("Missing NOTION_RELEASE_DB_ID env var")

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
