import os
from datetime import date
import requests
from dotenv import load_dotenv

# 1) Load the same secrets.env your test file uses
#    (absolute path so imports don't break it)
load_dotenv(dotenv_path="/home/jorge/projects/quant-trad/secrets.env")

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_RELEASE_DB_ID = os.getenv("NOTION_RELEASE_DB_ID")

# Optional debug – run once to confirm
print("DEBUG token present:", bool(NOTION_TOKEN))
print("DEBUG db id:", NOTION_RELEASE_DB_ID)

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


def create_release_page(
    name: str,
    change_type: str,
    summary: str,
    raw_changelog: str,
    priority: str = "Medium",
    status: str = "Released",
    release_date: date | None = None,
    version_tag: str | None = None,
):
    if release_date is None:
        release_date = date.today()

    payload = {
        "parent": {"database_id": NOTION_RELEASE_DB_ID},
        "properties": {
            "Release Name": {
                "title": [{"text": {"content": name}}],
            },
            "Change Type": {
                "select": {"name": change_type},
            },
            "Release Date": {
                "date": {"start": release_date.isoformat()},
            },
            "Priority": {
                "select": {"name": priority},
            },
            "Release Status": {
                "status": {"name": status},
            },
            "Summary": {
                "rich_text": [{"text": {"content": summary}}],
            },
            "Raw Changelog": {
                "rich_text": [{"text": {"content": raw_changelog}}],
            },
            "Tag / Version": {
                "rich_text": [{"text": {"content": version_tag or ""}}],
            },
        },
    }

    if version_tag:
        payload["properties"]["Tag / Version"] = {
            "rich_text": [{"text": {"content": version_tag}}]
        }

    resp = requests.post(
        "https://api.notion.com/v1/pages",
        headers=HEADERS,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()
