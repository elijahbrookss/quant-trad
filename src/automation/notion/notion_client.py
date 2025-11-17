import os
from datetime import date
from pathlib import Path
import requests
from dotenv import load_dotenv

# 1) Load the same secrets.env your test file uses
#    (absolute path so imports don't break it)
#load_dotenv(dotenv_path="/home/jorge/projects/quant-trad/secrets.env")
# parents[0] = notion
# parents[1] = automation
# parents[2] = src
# parents[3] = quant-trad (repo root)
BASE_DIR = Path(__file__).resolve().parents[3]
ENV_PATH = BASE_DIR / "secrets.env"

load_dotenv(dotenv_path=ENV_PATH)

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
    summary: str,
    release_date: date,
    branch: str,
):
    if release_date is None:
        release_date = date.today()
    payload = {
        "parent": {"database_id": NOTION_RELEASE_DB_ID},
        "properties": {
            "Name": {
                "title": [{"text": {"content": name}}],
            },
            "Release Date": {
                "date": {"start": release_date.isoformat()},
            },
            "Summary": {
                "rich_text": [{"text": {"content": summary}}],
            },
            "Branch": {
                "rich_text": [{"text": {"content": branch}}],
            },
        }
    }
    resp = requests.post(
        "https://api.notion.com/v1/pages",
        headers=HEADERS,
        json=payload,
    )
    
    
    print("DEBUG status:", resp.status_code)
    print("DEBUG body:", resp.text)
    
    resp.raise_for_status()
    return resp.json()
