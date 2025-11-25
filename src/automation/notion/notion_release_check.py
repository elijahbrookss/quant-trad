# notion_release.py
from automation.notion.notion_client import create_release_entry
from datetime import datetime
import json
import sys

def main(changelog_json_path: str):
    with open(changelog_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Assume your LLM output JSON looks like this:
    # {
    #   "version": "v0.3.1",
    #   "summary": "Add changelog -> Notion release logging",
    #   "details": "Long-form bullet list / paragraphs",
    #   "repo": "quant-trad-bot",
    #   "environment": ["prod"],
    #   "diff_url": "https://github.com/..."
    # }

    create_release_entry(
        version=data["version"],
        summary=data["summary"],
        changelog_text=data["details"],
        repo=data.get("repo"),
        environment=data.get("environment"),
        diff_url=data.get("diff_url"),
        released_at=datetime.utcnow(),
    )

if __name__ == "__main__":
    main(sys.argv[1])
