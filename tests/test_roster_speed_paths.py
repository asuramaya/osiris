"""The pieces that made roster() slow, each pinned: a settings object rebuilt on every call, the
450 KB trust table parsed once per seat, and the triage bucket read aggregating the whole
graph for sixty rows."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.config import settings as settings_mod
from src.orchestrator import compositions
from src.orchestrator.workspace_trust import load_projects, untrusted_dirs


def test_get_settings_is_built_once_while_its_inputs_are_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = 0
    real = settings_mod.Settings.__init__

    def counting(self: Any, *a: Any, **kw: Any) -> None:
        nonlocal built
        built += 1
        real(self, *a, **kw)

    monkeypatch.setattr(settings_mod.Settings, "__init__", counting)
    settings_mod._settings_cache = None
    for _ in range(5):
        settings_mod.get_settings()
    assert built == 1


def test_get_settings_notices_an_environment_change_and_hands_out_copies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_TRANSCRIPTS", "/tmp/first")
    first = settings_mod.get_settings()
    assert first.osiris_transcripts == "/tmp/first"
    monkeypatch.setenv("OSIRIS_TRANSCRIPTS", "/tmp/second")
    assert settings_mod.get_settings().osiris_transcripts == "/tmp/second"
    # one caller changing its object never shows up in the next caller's
    first.osiris_transcripts = "/tmp/mutated"
    assert settings_mod.get_settings().osiris_transcripts == "/tmp/second"


def test_untrusted_dirs_can_take_the_parsed_trust_table_instead_of_rereading_it(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    proj = tmp_path / "proj"
    proj.mkdir()
    (home / ".claude.json").write_text(json.dumps(
        {"projects": {str(proj): {"hasTrustDialogAccepted": False}}}))
    table = load_projects(home)
    assert untrusted_dirs([str(proj)], home) == [str(proj)]
    assert untrusted_dirs([str(proj)], projects=table) == [str(proj)]
    assert untrusted_dirs([str(proj)], projects=None) == []  # an unreadable table accuses nobody


async def test_the_type_scoped_triage_read_gives_the_same_buckets_as_the_whole_graph_read(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("SoftwareProject", "repo:scopeda", "test")
    b = await actions.create_or_find_object("SoftwareProject", "repo:scopedb", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:scopedorphan", "test")
    d = await actions.create_or_find_object("Decision", "decision:scoped1", "test")
    await actions.assert_property(a, "name", "scopeda", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(d, a, "in_repo", "test", now, 0.9)
    await actions.create_link(d, b, "in_repo", "test", now, 0.9)
    args = {"mode": "buckets", "object_type": "SoftwareProject", "status": "active",
            "limit": 2000}
    scoped = await compositions._fn_triage(actions.pool, None, dict(args))
    monkeypatch.setattr(compositions, "_TRIAGE_LINK_CTE_FOR_TYPE", compositions._TRIAGE_LINK_CTE)
    whole = await compositions._fn_triage(actions.pool, None, dict(args))
    assert scoped and json.dumps(scoped, sort_keys=True, default=str) == \
        json.dumps(whole, sort_keys=True, default=str)
