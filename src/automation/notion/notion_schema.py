# src/automation/notion/notion_schema.py
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional


# --- Property name constants (only place you ever change column names) --
# If you rename a column in Notion, change it ONLY here.
# "{Our given table name}" : "{Notion side column name}""

RELEASE_DB_PROPERTIES = {
    "name": "Release Name",      # Title column
    "summary": "Summary",        # Text
    "release_date": "Release Date",  # Date
    "branch": "Branch",          # Text
    "json_obj": "Metadata JSON",    # Text (optional)
}

RESPONSE_CHILD_PAGE_TITLE = "Changelog Responses"


# --- Builders: how we TALK to the DB schema ----------------------------

def build_release_properties(
    *,
    name: str,
    summary: str,
    release_date: date,
    branch: str,
    json_obj: str | None = None,
) -> dict:
    """
    Return the Notion 'properties' payload for a Release row.
    If you add/remove columns later, change ONLY this function +
    the PROP_* constants above.
    """
    p = RELEASE_DB_PROPERTIES
    
    props: Dict[str, Any] = {
        p["name"]: {
            "title": [
                {"type": "text", "text": {"content": name}}
            ]
        },
        p["summary"]: {
            "rich_text": [
                {"type": "text", "text": {"content": summary}}
            ]
        },
        p["release_date"]: {
            "date": {"start": release_date.isoformat()}
        },
        p["branch"]: {
            "rich_text": [
                {"type": "text", "text": {"content": branch}}
            ]
        },
    }

    if json_obj is not None:
        props[json_obj] = {
            "rich_text": [
                {"type": "text", "text": {"content": json_obj}}
            ]
        }

    return props



def build_response_blocks(
    *,
    social_post: str,
    dev_post: str,
    json_obj: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Blocks that go inside the release page as the 'response dump'."""
    blocks: List[Dict[str, Any]] = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {"type": "text", "text": {"content": "Changelog Responses"}}
                ]
            },
        },
        {
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [
                    {"type": "text", "text": {"content": "Social media post"}}
                ]
            },
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": social_post}}
                ]
            },
        },
        {
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [
                    {"type": "text", "text": {"content": "Dev Oriented post"}}
                ]
            },
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": dev_post}}
                ]
            },
        },
    ]

    if json_obj is not None:
        blocks.extend(
            [
                {
                    "object": "block",
                    "type": "heading_3",
                    "heading_3": {
                        "rich_text": [
                            {"type": "text", "text": {"content": "Metadata JSON"}}
                        ]
                    },
                },
                {
                    "object": "block",
                    "type": "code",
                    "code": {
                        "language": "json",
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": json_obj},
                            }
                        ],
                    },
                },
            ]
        )

    return blocks