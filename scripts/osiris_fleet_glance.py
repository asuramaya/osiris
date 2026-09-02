"""The fleet glance — the statusline for harnesses that have no statusline (task #28).

Crush has no statusLine config, no SessionStart hook, no TUI plugin surface — so the
Atlas seat ran blind: no ambient mail count, no fleet pulse, no model-vs-intent check.
This prints the SAME vitals osiris_hook.py's `statusline` subcommand renders (the
per-purpose osiris_statusline.py it once named was retired at the hook migration,
dispatch 5441/5599), as plain text on demand (the honest trade: no refresh interval, the
mind or the operator invokes it — the office's /fleet skill wraps exactly this script).

ONE AUTHORITY PER FACT (operator ruling 2026-07-19): every count comes from the shared
formulas in src/orchestrator/{mailbox,vitals} and src/orchestrator/ceiling — the same
functions orient, the pulse, the chrome, and the Claude statusline call. This script
owns no count SQL of its own.

Identity is the OFFICE, not a session: the newest mount row at this cwd is the resident
(after mount() that is the caller); the .osiris pin is the seat's declared intent. When
the row's model diverges from the pin, the glance says so plainly — including the case
where the graph has never met the caller at all (the knock: mount first).
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
EXPECTED = os.environ.get("OSIRIS_EXPECTED_MODEL", "claude-fable-5")
LEASE_SECS = 900  # mirror osiris_mail_lease_secs — deliverable = unsettled + no live lease


def _pin(cwd: Path) -> tuple[str, str]:
    """(project, model intent) from the office's .osiris pin, walking up; the dir name
    and the box default as fallbacks."""
    try:
        import tomllib
        for d in (cwd, *cwd.parents):
            f = d / ".osiris"
            if f.is_file():
                t = tomllib.loads(f.read_text())
                return (str(t.get("project") or cwd.name).strip(),
                        str(t.get("model") or EXPECTED).strip())
            if (d / ".git").exists():
                break
    except Exception:  # noqa: BLE001 — a glance never breaks on a config file
        pass
    return cwd.name, EXPECTED


async def _glance(cwd: Path) -> list[str]:
    import asyncpg

    project, intent = _pin(cwd)
    conn = await asyncpg.connect(
        DSN, timeout=2.0,
        server_settings={"application_name": "osiris-hook:fleet-glance"})
    try:
        row = await conn.fetchrow(
            "SELECT agent_id, model FROM agent_mounts WHERE cwd=$1 "
            "ORDER BY last_seen DESC LIMIT 1", str(cwd))
        agent = row["agent_id"] if row else None

        from src.config.settings import get_settings
        from src.orchestrator import surface, vitals
        from src.orchestrator.ceiling import ceiling
        from src.orchestrator.mailbox import desk_briefs_from, unread_split

        desk = await desk_briefs_from(conn, agent)
        split = await unread_split(conn, project, reader_agent=agent,
                                   lease_secs=LEASE_SECS)
        debts = await vitals.operator_debts(conn, hood=project)
        souls = await vitals.live_souls(conn)
        wakes = await vitals.wakes_hour(conn)
        ceil = await ceiling(conn, cap=get_settings().osiris_daily_usd)
        # was a fourth hand-copy of the sick predicate (Thoth msg 6327) — this module's own
        # docstring already claims "no count SQL of its own"; this line makes that true here too.
        sick = await surface._sensing(conn)

        lines = [f"◈ {project} — osiris fleet glance"]
        if sick:
            lines.append(f"⚠ NOT SENSING: {', '.join(sick[:3])} — the graph has "
                         "quietly stopped forming memory; tell the operator")
        mail_bits = [f"mail {split['mail']}"]
        if split.get("dm"):
            mail_bits.append(f"✉{split['dm']} DM(s) addressed to you")
        lines.append(" · ".join(mail_bits)
                     + (" — inbox() to read; reply or ack to settle"
                        if (split["mail"] or split.get("dm")) else ""))
        lines.append(f"owe here {debts['owed_here']} · fleet-wide owed {debts['owed']}")
        if desk:
            lines.append(f"briefs {desk} unread on the operator's desk")
        spend = "$∞" if ceil.cap <= 0 else f"${ceil.spent:.2f}/${ceil.cap:.0f}"
        blind = f" · ⚠{ceil.blind} unpriced" if ceil.blind else ""
        lines.append(f"fleet {souls['souls']} live · wakes {wakes}/h · {spend} day{blind}")

        if row is None:
            lines.append(f"⚠ the graph has no mount at this office — you are invisible "
                         f"to the fleet. Knock first: mount(cwd='{cwd}'), then orient().")
        else:
            model = (row["model"] or "?").strip()
            if model == intent:
                lines.append(f"model {model} — matches the seat's pin")
            else:
                lines.append(
                    f"⚠ newest mount here wears {model}, the pin declares {intent} — "
                    "if that mount is not you, mount(cwd=...) so the graph meets you; "
                    "if it is you, confess the divergence to the operator")
        return lines
    finally:
        await conn.close()


def main() -> None:
    cwd = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd()
    try:
        lines = asyncio.run(_glance(cwd))
    except Exception:  # noqa: BLE001 — the graph being down is information, not an error
        # NAME WHAT FAILED, NOT WHAT YOU GUESS CAUSED IT. This used to lead with "is
        # osiris-pg up? (docker ps…)" — which, on 2026-09-01, sent the operator after a
        # container that had been up 27 hours while the real cause was an osiris-mcp
        # restart invalidating his statusline's probe. A remedy that cannot distinguish
        # two states and confidently prescribes one is worse than no remedy (#169).
        # Order the checks nearest-first: the reader hits the likely one before the
        # unlikely one, instead of starting at the bottom of the stack.
        lines = [f"◈ {cwd.name}", "this read got no answer — the graph itself may be fine. "
                 "Check in order: systemctl --user status osiris-mcp (a deploy restarts "
                 "it), then docker ps for osiris-pg."]
    print("\n".join(lines))


if __name__ == "__main__":
    main()
