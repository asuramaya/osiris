"""The connection-envelope section of docs/DEPLOY.md quotes pool sizes and arithmetic;
this keeps those numbers tied to the settings and to pg_autotune's derivation, so a
change to either one cannot leave the doc quoting the old value (it did once: the doc
cited 20 and 16 long after the defaults became 8 and 4)."""
from __future__ import annotations

import inspect
import re
from pathlib import Path

from src.config.settings import Settings
from src.orchestrator.pg_autotune import compute_recommended

_DOC = (Path(__file__).resolve().parent.parent / "docs" / "DEPLOY.md").read_text()
_SECTION = _DOC.split("## The connection envelope at scale", 1)[1].split("\n## ", 1)[0]
_FIELD = {"osiris-mcp": "osiris_mcp_pool_size", "osiris-worker": "osiris_worker_pool_size",
          "osiris-console": "osiris_api_pool_size", "osiris-manager": "osiris_manager_pool_size"}
_RESERVED = 3  # Postgres superuser_reserved_connections, as the doc states
_HEARTBEAT_P50 = 0.057
_STOP_P50 = 0.016


def _default(field: str) -> int:
    value = Settings.model_fields[field].default
    assert isinstance(value, int)
    return value


def _doc_pools() -> dict[str, int]:
    rows = re.findall(r"^\| `(osiris-[a-z]+)` (?:\(api\) )?\| (\d+) \|", _SECTION, flags=re.M)
    return {name: int(n) for name, n in rows}


def _budget() -> int:
    return sum(_default(f) for f in _FIELD.values())


def _max_connections() -> int:
    headroom = inspect.signature(compute_recommended).parameters["headroom_target"].default
    return _budget() + int(headroom)


def test_pool_table_matches_the_settings_defaults() -> None:
    assert _doc_pools() == {name: _default(f) for name, f in _FIELD.items()}


def test_fixed_budget_and_max_connections_match() -> None:
    m = re.search(r"\| \*\*Fixed budget\*\* \| \*\*(\d+) / (\d+)\*\* \|", _SECTION)
    assert m, "the fixed-budget row is missing"
    assert (int(m.group(1)), int(m.group(2))) == (_budget(), _max_connections())
    assert f"`max_connections = {_max_connections()}`" in _SECTION


def test_headroom_is_max_minus_budget_minus_reserved() -> None:
    m = re.search(r"\*\*(\d+) connections of headroom\*\*", _SECTION)
    assert m
    assert int(m.group(1)) == _max_connections() - _budget() - _RESERVED


def test_route_ceilings_follow_the_mcp_pool() -> None:
    pool = _default("osiris_mcp_pool_size")
    hb = re.search(r"`/heartbeat`: (\d+) connections / 0\.057s is about \*\*(\d+) req/s\*\*",
                   _SECTION)
    st = re.search(r"`/stop`: (\d+) connections / 0\.016s is about \*\*(\d+) req/s\*\*",
                   _SECTION)
    assert hb and st
    assert int(hb.group(1)) == int(st.group(1)) == pool
    assert abs(int(hb.group(2)) - pool / _HEARTBEAT_P50) <= 1
    assert abs(int(st.group(2)) - pool / _STOP_P50) <= 5


def test_render_cadence_table_is_the_arithmetic_for_1000_sessions() -> None:
    ceiling = _default("osiris_mcp_pool_size") / _HEARTBEAT_P50
    rows = re.findall(r"^\| every (\d+)s[^|]*\| ~(\d+) req/s \|", _SECTION, flags=re.M)
    assert rows, "the cadence table is missing"
    for seconds, demand in rows:
        assert int(demand) == round(1000 / int(seconds))
    # the summary's cadence is the first row that sits comfortably under the ceiling
    comfortable = [int(s) for s, d in rows if int(d) <= 0.5 * ceiling]
    assert comfortable and min(comfortable) == 15
    assert "about 15 seconds or slower" in _SECTION


def test_the_old_pre_cut_caps_appear_only_as_history() -> None:
    # 20 and 16 may be named, but only next to the measurement they belong to
    for m in re.finditer(r"(?:`osiris-mcp`|`osiris-worker`) (?:20|16)\b", _SECTION):
        window = _SECTION[max(0, m.start() - 400):m.end() + 400]
        assert "earlier" in window or "measurement" in window or "cut" in window
