from datetime import date
from .notion_client import create_release_page
import json
if __name__ == "__main__":
    page = create_release_page(
        name="v0.0.4 Single DB responses",
        summary="Testing responses stored in the release page body.",
        branch="feature/single-db",
        release_date=date.today(),
        #social_post="We now store response content directly inside the release page ",
    )

    print("Created release page:", page["id"])