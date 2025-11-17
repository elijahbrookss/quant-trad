from datetime import date
from notion_client import create_release_page

if __name__ == "__main__":
    page = create_release_page(
        name="Test Release via Script",
        summary="Testing simple Notion schema wiring.",
        release_date=date.today(),
        branch="feature/jorge/notion-integration",
    )
    print("Created page:", page["id"])
