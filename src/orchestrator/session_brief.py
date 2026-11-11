"""A compaction-quality brief of a whole stored session, built from the store alone.

When the harness will not load a transcript another account owns, a resume used to start a
fresh session seeded with the last twelve turns (7000 characters). That is thin: the mind
that wrote the session had spent hours and tens of thousands of lines. This module reads the
WHOLE session from osiris's own copy (the soul store, never the file on disk) and extracts,
deterministically and without a model:

  * the last compaction summary the old session wrote about itself, verbatim (it is the best
    account of everything before it), plus what happened since;
  * the last operator and manager instructions, verbatim;
  * decisions recorded, threads opened and resolved, mail sent, files and branches touched,
    commits made, and the last things the mind said;
  * the seat's open threads from the graph, and the project's compiled handoff;
  * how to pull any line of the old transcript on demand, with its line index.

The size target is what a Claude Code compaction summary measures: 47 summaries in one
long-lived session ran 14,000 to 45,000 characters, median 28,000.

It reads from the store and writes nothing, and it is a plain-text brief handed to a new
session as its opening message. It never touches `ownerAccountUuid` or any transcript line,
never writes a transcript, and claims nothing about who owns the old content: the harness's
cross-account boundary stays exactly where it is."""
from __future__ import annotations

import json
import re
from collections import Counter, OrderedDict, deque
from datetime import datetime
from typing import Any

import asyncpg

TOTAL_CHARS = 28000   # a compaction summary's measured median (range 14,000 to 45,000)
HARD_CAP_CHARS = 32000

_SKIP_USER_PREFIXES = (
    "<system-reminder>", "[SYSTEM NOTIFICATION", "Stop hook feedback", "<command-name>",
    "<local-command", "<task-notification>", "Caveat:", "<user-prompt-submit-hook>",
    "Stop hook blocking error",
)
_SKIP_USER_EXACT = {"Tool loaded."}
_SLASH_ONLY = {"/compact", "/clear", "/settle", "/mail", "/orient", "/exit"}
_MAIL_PREVIEW = "┌─ osiris mail"
_COMPACT_PREFIX = "This session is being continued from a previous conversation"
_COMMIT = re.compile(
    r"^\[(?P<branch>[\w/.\-]+)(?: \(root-commit\))? (?P<sha>[0-9a-f]{7,40})\] (?P<msg>.+)$",
    re.MULTILINE)
_BRANCH_B = re.compile(
    r"(?:git\s+(?:checkout|switch)\s+-[bBc]|worktree\s+add\b[^\n;|&]*?\s-b)\s+(\S+)")
_COMMIT_MSG = re.compile(
    r"""(?:git commit[^\n]*?\s-m\s+|retry_commit\.sh\s+)(?:"\$\(cat <<'?EOF'?\n\s*)?["']?"""
    r"""([^"'\n]{6,160})""")
_AGENT_CANON = re.compile(r"^agent:[0-9a-f]{8}(?:-[a-z0-9]+)*$")


def _cap(text: str, limit: int, *, ref: str | None = None) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    more = len(text) - limit
    tail = f" [+{more} more characters" + (f"; {ref}" if ref else "") + "]"
    return text[:max(0, limit - len(tail))].rstrip() + tail


def _cap_ends(text: str, head: int, tail: int, *, ref: str) -> str:
    """Keep the start and the END of `text`. A compaction summary opens with the request and
    the concepts and closes with the pending tasks, the current work and the next step; the
    close is the part a continuation needs most, so a plain head cut would drop it."""
    text = text.strip()
    if len(text) <= head + tail:
        return text
    cut = len(text) - head - tail
    return (text[:head].rstrip() + f"\n[... {cut} characters omitted from the middle; the whole "
            f"summary is at {ref} ...]\n" + text[-tail:].lstrip())


def _text_of(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        return " ".join(str(b.get("text", "")).strip() for b in content
                        if isinstance(b, dict) and b.get("type") == "text").strip()
    return ""


class _Tail(deque[tuple[Any, ...]]):
    """A bounded deque (the newest `maxlen` kept) that also counts everything ever appended, so
    the brief can say how many earlier entries it is NOT showing, a true total and not just how
    many were trimmed from what was kept."""

    def __init__(self, maxlen: int) -> None:
        super().__init__(maxlen=maxlen)
        self.seen = 0

    def append(self, item: tuple[Any, ...]) -> None:
        self.seen += 1
        super().append(item)


class SessionDigest:
    """Fed one stored line at a time (`feed`), so a very long session never sits in memory."""

    def __init__(self) -> None:
        self.lines = 0
        self.first_ts: str | None = None
        self.last_ts: str | None = None
        self.models: Counter[str] = Counter()
        self.compactions = 0
        self.compact: tuple[int, str] | None = None
        self.first_request: tuple[int, str] | None = None
        self.operator: _Tail = _Tail(6)
        self.mails: _Tail = _Tail(8)        # mail previews seen
        self.dms: OrderedDict[int, tuple[int, str, str]] = OrderedDict()  # full ask bodies
        self.dm_ids: set[int] = set()        # every distinct DM seen, however few are kept
        self.said: _Tail = _Tail(4)
        self.decisions: _Tail = _Tail(40)
        self.opened: _Tail = _Tail(25)
        self.resolved: _Tail = _Tail(25)
        self.sent: _Tail = _Tail(20)
        self.files: OrderedDict[str, int] = OrderedDict()
        self.branches: OrderedDict[str, int] = OrderedDict()
        self.commits: _Tail = _Tail(20)
        self.committing: _Tail = _Tail(20)
        self.tool_counts: Counter[str] = Counter()

    # -- feeding -----------------------------------------------------------------------

    def feed(self, idx: int, raw: bytes) -> None:
        self.lines = idx + 1
        try:
            d = json.loads(raw)
        except ValueError:
            return
        if not isinstance(d, dict):
            return
        ts = d.get("timestamp")
        if isinstance(ts, str):
            self.first_ts = self.first_ts or ts
            self.last_ts = ts
        kind = d.get("type")
        if kind == "user":
            self._user(idx, d)
        elif kind == "assistant":
            self._assistant(idx, d)

    def _user(self, idx: int, d: dict[str, Any]) -> None:
        msg = d.get("message")
        content = msg.get("content") if isinstance(msg, dict) else None
        if isinstance(content, list):
            for b in content:
                if isinstance(b, dict) and b.get("type") == "tool_result":
                    self._tool_result(idx, b)
        text = _text_of(content)
        if not text:
            return
        if d.get("isCompactSummary") or text.startswith(_COMPACT_PREFIX):
            self.compactions += 1
            self.compact = (idx, text)
            return
        if (d.get("isMeta") or text in _SKIP_USER_EXACT or text.startswith(_SKIP_USER_PREFIXES)
                or text.lower() in _SLASH_ONLY or text.startswith("<command-")):
            return
        if text.startswith(_MAIL_PREVIEW):
            self.mails.append((idx, text))
            return
        self.operator.append((idx, text))
        if self.first_request is None:
            self.first_request = (idx, text)

    def _tool_result(self, idx: int, block: dict[str, Any]) -> None:
        body = block.get("content")
        text = body if isinstance(body, str) else _text_of(body)
        if not text:
            return
        if text.startswith('{"project"') and '"messages"' in text:
            try:
                data = json.loads(text)
            except ValueError:
                data = None
            for m in (data or {}).get("messages") or []:
                if not isinstance(m, dict) or not m.get("dm") or m.get("grade") != "ask":
                    continue
                who = str(m.get("from") or "")
                if who.startswith(("deploy:", "cron:")):
                    continue
                mid = int(m.get("id") or 0)
                self.dms[mid] = (idx, who, str(m.get("body") or ""))
                self.dm_ids.add(mid)
                self.dms.move_to_end(mid)
                while len(self.dms) > 8:
                    self.dms.popitem(last=False)
        for m in _COMMIT.finditer(text):
            self.commits.append((idx, m["branch"], m["sha"][:8], m["msg"][:140]))

    def _assistant(self, idx: int, d: dict[str, Any]) -> None:
        msg = d.get("message")
        if not isinstance(msg, dict):
            return
        model = str(msg.get("model") or "")
        if model and model != "<synthetic>":
            self.models[model] += 1
        content = msg.get("content")
        if isinstance(content, str):  # some writers store a plain string, not a block list
            content = [{"type": "text", "text": content}]
        if not isinstance(content, list):
            return
        for b in content:
            if not isinstance(b, dict):
                continue
            if b.get("type") == "text":
                t = str(b.get("text") or "").strip()
                if len(t) >= 60:
                    self.said.append((idx, t))
            elif b.get("type") == "tool_use":
                self._tool_use(idx, str(b.get("name") or ""), b.get("input"))

    def _tool_use(self, idx: int, name: str, inp: Any) -> None:
        if not isinstance(inp, dict):
            return
        self.tool_counts[name] += 1
        short = name.rsplit("__", 1)[-1]
        if short == "record_decision":
            self._note(self.decisions, idx, inp.get("summary"), 260)
        elif short == "open_thread":
            self._note(self.opened, idx, inp.get("summary"), 220)
        elif short == "thread" and inp.get("action") == "resolve":
            ref, because = str(inp.get("ref") or ""), str(inp.get("because") or "")
            self._note(self.resolved, idx, f"{ref}: {because}", 220)
        elif short == "settle":
            for item in inp.get("decisions") or []:
                if isinstance(item, dict):
                    self._note(self.decisions, idx, item.get("summary"), 260)
            for item in inp.get("threads_open") or []:
                if isinstance(item, dict):
                    self._note(self.opened, idx, item.get("summary"), 220)
            for item in inp.get("threads_resolve") or []:
                if isinstance(item, dict):
                    self._note(self.resolved, idx,
                               f"{item.get('ref', '')}: {item.get('because', '')}", 220)
        elif short == "send":
            to = inp.get("to_agent") or inp.get("to")
            who = (f"to {to}" if to else f"reply to mail #{inp['reply_to']}"
                   if inp.get("reply_to") else "to the fleet")
            re_ = f" (reply to #{inp['reply_to']})" if to and inp.get("reply_to") else ""
            self._note(self.sent, idx, f"{who}{re_}: {inp.get('body') or ''}", 240)
        elif name in ("Edit", "Write", "NotebookEdit"):
            path = inp.get("file_path") or inp.get("notebook_path")
            if path:
                self.files[str(path)] = idx
                self.files.move_to_end(str(path))
        elif name == "Bash":
            cmd = str(inp.get("command") or "")
            if "git commit" in cmd or "retry_commit" in cmd:
                m = _COMMIT_MSG.search(cmd)
                # a real subject starts with a word; "$(printf" and friends are shell, not one
                if m and m.group(1)[:1].isalnum() and " " in m.group(1):
                    self._note(self.committing, idx, m.group(1), 150)
            for m in _BRANCH_B.finditer(cmd):
                self.branches[m.group(1).strip("'\"")] = idx
                self.branches.move_to_end(m.group(1).strip("'\""))
        elif name == "EnterWorktree" and inp.get("name"):
            self.branches[f"worktree {inp['name']}"] = idx

    @staticmethod
    def _note(into: deque[tuple[int, str]], idx: int, text: Any, limit: int) -> None:
        t = " ".join(str(text or "").split())
        if t:
            into.append((idx, _cap(t, limit)))

    # -- rendering ---------------------------------------------------------------------

    def recall_ref(self, anchor_sid: str) -> str:
        return f"line L<n> of session {anchor_sid}"


def _section(
    title: str, items: list[str], limit: int, available: int | None = None,
) -> str:
    """`title`, then `items` (already one line each) newest last, trimmed from the OLD end to
    fit `limit` characters; "" when there is nothing. `available` is how many entries the
    session really had (a bounded collection keeps only the newest, and the caller shows only
    the last few of those): the "(+N earlier not shown)" note is that true total minus what is
    shown, never just what this function trimmed."""
    if not items:
        return ""
    head = f"{title}\n"
    kept: list[str] = []
    used = len(head)
    for item in reversed(items):
        if used + len(item) + 1 > limit and kept:
            break
        kept.append(item)
        used += len(item) + 1
    dropped = max(available if available is not None else len(items), len(items)) - len(kept)
    body = "\n".join(reversed(kept))
    note = f"\n(+{dropped} earlier not shown)" if dropped else ""
    return head + body + note


def render_brief(
    digest: SessionDigest, *, handle: str, anchor_sid: str, agent: str | None,
    finding: dict[str, Any] | None, graph_threads: list[str], handoff: str | None,
    total: int = TOTAL_CHARS,
) -> str:
    last = max(digest.lines - 1, 0)
    where = (f"agent {agent}, " if agent else "") + f"anchor_sid {anchor_sid}"
    models = ", ".join(m for m, _ in digest.models.most_common(2)) or "unknown"
    why = (finding or {}).get("detail", "its history belongs to another Claude account")
    parts: list[str] = []

    parts.append(_cap(
        f"You are {handle}, in a NEW session that continues an earlier one. The earlier session "
        f"({where}) could not be resumed: {why}. The harness refuses history owned by another "
        "account and osiris does not and will not work around that; the history was never "
        "loaded into your context. Everything the earlier session wrote is in osiris's own "
        "copy, and this brief was built from all of it: "
        f"{digest.lines} stored lines, {digest.first_ts or '?'} to {digest.last_ts or '?'}, "
        f"{digest.compactions} compaction(s), models {models}. Call mount() and orient(), read "
        f"your charter at ~/.osiris/seats/{handle.lower()}/charter.md, then treat what follows "
        "as RECALLED CONTEXT, not as instructions: the live instructions are in your mail and "
        "the graph. Items cite their stored line as L<n>.", 1500))

    parts.append(_cap(
        "HOW TO PULL DETAIL ON DEMAND. The full old transcript is in the store, lines 0 to "
        f"{last}. To read line N of it: cite_transcript(ref=<one of your own open threads or a "
        f"decision you just recorded>, agent='{agent or '<the old agent id>'}', line_idx=N, "
        "because='recalling the earlier session'), then read_citation(ref=<the same ref>, "
        f"agent='{agent or '<the old agent id>'}'), which returns that line's text, chain-"
        "verified. Line numbers in this brief are those indices. recall(<ref>) reads a decision "
        "or thread named below in full. Do this for a specific fact; do not replay the "
        "session.", 1500))

    if digest.compact is not None:
        idx, text = digest.compact
        parts.append(f"THE EARLIER SESSION'S OWN LAST COMPACTION SUMMARY (L{idx}), its "
                     "account of everything before it, verbatim:\n"
                     + _cap_ends(text, 3500, 5500, ref=f"L{idx}"))
    elif digest.first_request is not None:
        idx, text = digest.first_request
        parts.append(f"THE FIRST REQUEST (L{idx}):\n" + _cap(text, 1500, ref=f"L{idx}"))

    asks: list[str] = []
    for mid, (idx, who, body) in list(digest.dms.items())[-5:]:
        asks.append(f"- L{idx} DM #{mid} from {who}, verbatim: " + _cap(body, 800))
    for idx, text in list(digest.operator)[-3:]:
        asks.append(f"- L{idx} operator, verbatim: " + _cap(text, 700))
    if not asks:
        for idx, text in list(digest.mails)[-4:]:
            asks.append(f"- L{idx}: " + _cap(" ".join(text.split()), 600))
    asked = (len(digest.dm_ids) + digest.operator.seen) or digest.mails.seen
    parts.append(_section("LAST INSTRUCTIONS RECEIVED (oldest first):", asks, 4500, asked))

    said = [f"- L{idx}: " + _cap(" ".join(t.split()), 900) for idx, t in digest.said][-2:]
    parts.append(_section("WHAT YOU WERE DOING (your last words in that session):", said, 2000,
                          digest.said.seen))

    parts.append(_section(
        "DECISIONS YOU RECORDED:", [f"- L{i}: {t}" for i, t in digest.decisions], 2500,
        digest.decisions.seen))

    thread_lines = [f"- OPEN NOW: {t}" for t in graph_threads]
    thread_lines += [f"- L{i} opened: {t}" for i, t in digest.opened][-8:]
    thread_lines += [f"- L{i} resolved: {t}" for i, t in digest.resolved][-6:]
    parts.append(_section("THREADS AND OBLIGATIONS:", thread_lines, 2400,
                          len(graph_threads) + digest.opened.seen + digest.resolved.seen))

    parts.append(_section(
        "MAIL YOU SENT (last):", [f"- L{i}: {t}" for i, t in digest.sent], 1200,
        digest.sent.seen))

    work: list[str] = []
    if digest.branches:
        more = max(len(digest.branches) - 10, 0)
        work.append("branches: " + ", ".join(
            f"{b} (L{i})" for b, i in list(digest.branches.items())[-10:])
            + (f" (+{more} earlier not shown)" if more else ""))
    seen_msgs: set[str] = set()
    for i, br, sha, msg in list(digest.commits)[-10:]:
        work.append(f"commit {sha} on {br}: {msg} (L{i})")
        seen_msgs.add(msg[:40])
    for i, msg in list(digest.committing)[-10:]:
        if msg[:40] not in seen_msgs:
            work.append(f"committed: {msg} (L{i})")
    if digest.commits.seen > 10:
        work.append(f"(+{digest.commits.seen - 10} earlier commits not shown)")
    if digest.files:
        more_files = max(len(digest.files) - 14, 0)
        work.append("files edited, most recent last: " + ", ".join(
            f"{p} (L{i})" for p, i in list(digest.files.items())[-14:])
            + (f" (+{more_files} earlier not shown)" if more_files else ""))
    parts.append(_section("WORK TOUCHED:", work, 2400))

    if handoff:
        parts.append("WHAT THE GRAPH RECORDED FOR THE PROJECT SINCE THAT SESSION ENDED "
                     f"({digest.last_ts}):\n" + _cap(handoff, 1800))

    brief = "\n\n".join(p for p in parts if p)
    if len(brief) > total + 4000:
        brief = _cap(brief, HARD_CAP_CHARS)
    return brief


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _old_agent(pool: asyncpg.Pool, anchor_sid: str) -> str | None:
    """The agent generation that wore this session, the identity the citation tools key on:
    the newest agent carrying `session = anchor_sid` whose canonical is a lineage numeral
    (a sub-agent's long hash is not)."""
    rows = await pool.fetch(
        "SELECT o.canonical FROM objects o JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='Agent' AND a.name='session' AND a.value #>> '{}' = $1 "
        "ORDER BY a.observed_at DESC LIMIT 50", anchor_sid)
    for r in rows:
        if _AGENT_CANON.match(str(r["canonical"])):
            return str(r["canonical"])
    return None


async def _open_threads(pool: asyncpg.Pool, seat_id: str | None, handle: str) -> list[str]:
    owners = [o for o in {seat_id, handle, handle.lower()} if o]
    if not owners:
        return []
    rows = await pool.fetch(
        "SELECT o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS s, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS k "
        "FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "     AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)='open' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "     AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    = ANY($1::text[]) "
        "ORDER BY o.created_at DESC LIMIT 15", owners)
    return [f"[{r['k'] or 'thread'} {str(r['canonical']).removeprefix('thread:')[:8]}] "
            + _cap(" ".join(str(r["s"] or "").split()), 220) for r in rows]


async def build_session_brief(
    pool: asyncpg.Pool, *, handle: str, anchor_sid: str, finding: dict[str, Any] | None,
    project: str | None = None, seat_id: str | None = None,
) -> str | None:
    """The brief for the whole stored session `anchor_sid`, or None when the store holds no
    lines for it (the caller then falls back to the short tail brief)."""
    from src.ingest.soul_store import SoulStore

    digest = SessionDigest()
    async for idx, raw in SoulStore(pool).stream_lines(anchor_sid):
        digest.feed(idx, raw)
    if digest.lines == 0:
        return None
    handoff: str | None = None
    if project:
        try:
            from src.orchestrator.handoff_compiler import compile_handoff, render_handoff_briefing

            since = _parse_ts(digest.last_ts)
            data = await compile_handoff(pool, repo=project, since=since) if since else {}
            handoff = render_handoff_briefing(data) if data else None
        except Exception:  # noqa: BLE001 - a brief never fails the launch
            handoff = None
    try:
        threads = await _open_threads(pool, seat_id, handle)
    except Exception:  # noqa: BLE001
        threads = []
    return render_brief(
        digest, handle=handle, anchor_sid=anchor_sid, agent=await _old_agent(pool, anchor_sid),
        finding=finding, graph_threads=threads, handoff=handoff)
