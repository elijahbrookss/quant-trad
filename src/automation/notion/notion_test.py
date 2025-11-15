from datetime import date
from notion_client import create_release_page

if __name__ == "__main__":
    page = create_release_page(
        name="Test Release via Script",
        change_type="Feature",
        summary="Testing Notion integration wiring.",
        raw_changelog="- Added Notion pipeline\n- Fixed config loading",
        priority="High",
        status="Released",
        release_date=date.today(),
        version_tag="v0.0.1-dev",
    )
    print("Created page:", page["id"])
