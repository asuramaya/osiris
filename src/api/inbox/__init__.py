"""THE INBOX (task #71, ruling 0b3dd431) — slice one of the UI rebuild.

A closed, typed block vocabulary (blocks.py) composed by pure builders (inbox.py) into
trees, rendered to HTML by ONE frozen Jinja file (render.py + templates/catalog.html.j2).
No other code in this subpackage — or anywhere else in the app — is permitted to touch
Jinja or emit HTML/CSS directly; inventing a component is a mypy error, not a review
comment (research-architecture.md). Replaces the membrane at :8011 (src/api/membrane.py's
module stays in tree, unrouted, for one deploy cycle per Thoth's own instruction, msg 1811).
"""
from __future__ import annotations
