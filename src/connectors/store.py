"""Content-addressed local artifact store (replaces R2 on the self-hosted box).

Evidence (raw helper responses, scraped HTML, screenshots) is written under
OSIRIS_ARTIFACT_DIR keyed by sha256, giving each artifact an integrity anchor:
a cited evidence_uri can't be silently swapped without changing its hash.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class ArtifactStore:
    def __init__(self, base_dir: str) -> None:
        self.base = Path(base_dir)

    def put_json(self, obj: Any) -> tuple[str, str]:
        """Store a JSON-serializable artifact; return (uri, sha256)."""
        raw = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
        return self._put(raw, suffix=".json")

    def put_bytes(self, raw: bytes, *, suffix: str = "") -> tuple[str, str]:
        return self._put(raw, suffix=suffix)

    def _put(self, raw: bytes, *, suffix: str) -> tuple[str, str]:
        digest = hashlib.sha256(raw).hexdigest()
        # shard by first 2 hex chars to avoid one giant directory
        path = self.base / digest[:2] / f"{digest}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():  # content-addressed -> identical bytes need no rewrite
            path.write_bytes(raw)
        return f"file://{path.resolve()}", digest
