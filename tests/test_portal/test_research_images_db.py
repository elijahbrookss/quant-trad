"""Real transaction/custody proof; disposable database only."""
import base64
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from PIL import Image
import pytest
from sqlalchemy.exc import IntegrityError

from portal.backend.db.models import ResearchImageRecord, ResearchItemRecord
from portal.backend.db.session import db
from portal.backend.service.research import images, publication, repository

pytestmark = pytest.mark.db


def test_image_custody_retry_fresh_process_and_unchanged_publication(tmp_path):
    study = repository.create_item(kind="study", title="Disposable image custody " + uuid.uuid4().hex)
    claim = repository.create_item(kind="hypothesis", title="Synthetic reasoning")
    publication.adopt(study["id"], {"question": "What does the synthetic chart show?", "scope": "Disposable"})
    revision = publication.publish(study["id"], {
        "request_id": "first", "expected_previous_hash": None, "conclusion": "Illustration only",
        "limitations": "Synthetic test", "scope": "Disposable",
        "references": [{"item_id": claim["id"], "kind": "hypothesis", "role": "context",
                        "content_hash": publication.content_identity(claim)}],
    })
    before = repository.get_item(study["id"])
    stream = BytesIO()
    Image.new("RGB", (24, 12), "white").save(stream, format="PNG")
    local = tmp_path / "local.png"
    local.write_bytes(stream.getvalue())
    request = {"title": "Illustration", "explanation": "Synthetic test chart", "alt_text": "White rectangle",
               "binding": {"publication_hash": revision["publication_hash"]},
               "content_base64": base64.b64encode(local.read_bytes()).decode()}
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: images.attach(study["id"], request), range(2)))
    assert results[0] == results[1]
    image = results[0]
    local.unlink()
    assert repository.get_item(study["id"]) == before
    assert publication.history(study["id"])["publications"] == [revision]
    code = (
        "import json, hashlib; from portal.backend.service.research.images import get_content; "
        f"content, manifest = get_content({study['id']!r}, {image['image_id']!r}); "
        "print(json.dumps({'sha256': hashlib.sha256(content).hexdigest(), 'binding': manifest['binding']}))"
    )
    root = Path(__file__).resolve().parents[2]
    environment = {**os.environ, "PYTHONPATH": f"{root}:{root / 'src'}"}
    fresh = subprocess.run([sys.executable, "-c", code], cwd=root, env=environment,
                           capture_output=True, text=True, check=True, timeout=60)
    restored = json.loads(fresh.stdout.strip().splitlines()[-1])
    assert restored["sha256"] == image["sha256"]
    assert restored["binding"] == image["binding"]
    # A real SQL constraint failure rolls back without damaging retained bytes.
    with pytest.raises(IntegrityError):
        with db.session() as session:
            saved = session.get(ResearchImageRecord, image["image_id"])
            saved.content = b""
            session.flush()
    assert images.get_content(study["id"], image["image_id"])[1] == image
    with db.session() as session:
        saved = session.get(ResearchImageRecord, image["image_id"])
        saved.content = b"corrupt"
    with pytest.raises(ValueError, match="integrity_error"):
        images.get_content(study["id"], image["image_id"])
    # Test data cleanup; production has no image mutation/delete operation.
    with db.session() as session:
        session.delete(session.get(ResearchImageRecord, image["image_id"]))
        session.delete(session.get(ResearchItemRecord, study["id"]))
        session.delete(session.get(ResearchItemRecord, claim["id"]))
