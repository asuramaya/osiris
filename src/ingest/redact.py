"""Redaction — strike credential shapes from text before the LLM or the graph sees it.

Ruling f8f22e14: any text a miner or ingest path pulls into the graph — session dialogue,
a project's mds — may carry printed key material (env dumps, tokens, headers land verbatim).
This is the SHARED gate both the session-miner and the reference/bootstrap ingest run text
through. Conservative on purpose: short commit refs, UUIDs, and ordinary prose pass; anything
labelled or key-shaped is struck. A tiny, dependency-free module, so foundational ingest can
use it without importing the heavy session-miner (which pulls in the LLM providers).
"""
from __future__ import annotations

import re

# --- redaction (ruling f8f22e14): strike credential shapes BEFORE the LLM/graph sees text ---

_REDACTIONS: list[tuple[re.Pattern[str], str]] = [
    # Bearer first — the assignment rule below would otherwise eat the word "Bearer"
    # out of "Authorization: Bearer <token>" and leave the bare token standing.
    (re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}"), "Bearer [REDACTED]"),
    # labelled assignments: ETHERSCAN_API_KEY=..., "token": "...", Authorization: ...
    # The [\w-]* prefix matters: env names like ETHERSCAN_API_KEY have no word boundary
    # before "API", which is exactly the shape that leaked in the live transcript.
    (re.compile(
        r"""(?i)\b([\w-]*(?:key|token|secret|passwd|password|credential|authorization)"""
        r"""s?\b["']?\s*[:=]\s*)["']?[^\s"',;]{6,}"""),
     r"\1[REDACTED]"),
    # vendor key prefixes (Anthropic/OpenAI, GitHub, Slack, AWS, JWT)
    (re.compile(r"\bsk-(?:ant-)?[A-Za-z0-9_-]{12,}"), "[REDACTED-KEY]"),
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"), "[REDACTED-KEY]"),
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), "[REDACTED-KEY]"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "[REDACTED-KEY]"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9._-]{16,}"), "[REDACTED-JWT]"),
    # long opaque blobs. Full 40-hex SHAs go too — rationales cite SHORT refs, which
    # survive; UUIDs survive (dash every <=12 chars breaks the runs).
    (re.compile(r"\b[0-9a-fA-F]{40,}\b"), "[REDACTED-BLOB]"),
    (re.compile(r"\b[A-Za-z0-9+/=]{48,}\b"), "[REDACTED-BLOB]"),
    (re.compile(r"\b[A-Za-z0-9_-]{64,}\b"), "[REDACTED-BLOB]"),
]


def redact(text: str) -> str:
    """Strike credential-shaped spans from text. Conservative on purpose: short commit refs,
    UUIDs, and ordinary prose pass untouched; anything labelled or key-shaped is replaced
    before the model (or the graph) can see it."""
    for pat, repl in _REDACTIONS:
        text = pat.sub(repl, text)
    return text


_CRED_VALUE = re.compile(
    r"(?i)\[REDACTED|\bBearer\s+\S|\b[\w-]*(?:key|token|secret|password)\b\s*[:=]"
    r"|\b[A-Za-z0-9+/=]{32,}\b"
)


def credential_shaped(value: str) -> bool:
    """The emit-time hard gate: no extracted assertion may carry a credential shape —
    including a redaction marker (a summary BUILT AROUND a struck secret is still about
    the secret). Defense in depth behind `redact`."""
    return bool(_CRED_VALUE.search(value))


# --- the off-record sentinel (the panopticon seam, operator's forks answered 2026-07-19) ---
#
# THE TRUST CAPABILITY (thread 6fc061a0, design e4b713fd): a passage between paired
# ‹off-record› … ‹on-record› markers never becomes graph memory — stripped HERE, before
# any extractor LLM sees the text, the same pipeline seat as credential redaction.
# The operator's four rulings: BOTH operator and agents may mark; the reach is
# NOT-IN-GRAPH ONLY (the on-disk transcript keeps the passage — private notebook, shared
# record); paired markers at the finest grain; an UNCLOSED marker runs to the end of that
# message only — fail-safe, it can never eat the rest of a session. The glyphs are
# single-guillemet angle quotes (U+2039/U+203A), chosen because no code or prose produces
# them by accident. Completeness stays the DEFAULT: silence is captured; privacy is a
# deliberate act.

_OFF_RECORD = re.compile(r"‹off-record›.*?(?:‹on-record›|\Z)", re.DOTALL)


def strip_off_record(text: str) -> str:
    """Remove every ‹off-record›…‹on-record› span (unclosed → to the end of this text).
    Runs per message: cross-message spans are deliberately NOT honored — the fail-safe
    outranks convenience, and each message re-marks its own privacy."""
    return _OFF_RECORD.sub("", text)
