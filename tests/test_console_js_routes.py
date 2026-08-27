"""Static-source guards for console.js routes that have no browser test coverage.

tagIt() posted to /objects/:id/tag instead of the real /objects/:id/tags route, was fixed once
on a branch (khnum-land-console, 1867a5c), then silently lost when a legitimate full rewrite
(9e2226b) superseded that branch without carrying the one-line fix forward — untested, so
nothing caught it live again for three days (thread 98344adb). This file exists so the same
route can't regress invisibly a second time.
"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


def test_tag_it_posts_to_the_real_tags_route() -> None:
    assert "/objects/' + id + '/tags'" in _JS
    assert "/objects/' + id + '/tag'" not in _JS
