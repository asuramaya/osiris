from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. Single-operator self-hosted box; secrets via env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://osiris:osiris@127.0.0.1:5432/osiris"
    redis_url: str = "redis://127.0.0.1:6379/0"
    # Single static operator identity — fills the actor role CF Access would have provided.
    osiris_actor: str = "analyst:operator"
    osiris_artifact_dir: str = "./artifacts"
    # On-chain ingest. Etherscan v2 is the one base where "keyless" bends: the API
    # rejects unkeyed calls, but a free key lifts the whole limit. Empty => the ETH
    # connector degrades gracefully (returns an error dict, never crashes a run).
    etherscan_api_key: str = ""
    # The watch (cron Phase 3): comma-separated query terms to watch for new SEC Form D
    # filings (e.g. "Neuralink,Anthropic"). Empty => the worker registers no source tick
    # (the watch stays source-agnostic until an operator names a beat).
    osiris_watch_form_d: str = ""
    # AI extraction (cron Phase 4): the model used by the universal extractor. A
    # document→entities task is flash-tier; Opus would be wasteful per-filing.
    osiris_extract_model: str = "claude-haiku-4-5-20251001"
    anthropic_api_key: str = ""
    # Inference providers — the GPU-as-an-API-key abstraction (src/ingest/providers.py).
    # The engine never runs a GPU; the model is a hosted API (a key), the LOCAL claude CLI
    # (subscription-covered, no key), or a local GPU backend. `osiris_extract_provider`:
    # 'auto' (the default — prefer the LOCAL claude CLI if installed, else an API key) |
    # 'claude-cli' (force the installed Claude Code — the core box, no key) | 'anthropic'
    # (force an API key — satellites/remote with no CLI) | 'none'. So the core box "just
    # works" keyless off its own Claude, while a satellite uses its key. `osiris_vision_model`
    # OCRs a scanned page → text before extraction (county notices are scans).
    osiris_extract_provider: str = "auto"
    osiris_claude_binary: str = "claude"
    osiris_vision_model: str = "claude-haiku-4-5-20251001"
    # Semantic search (the max-level ruling a0cfcca1). The Claude CLI has no embeddings
    # endpoint and keyless is a feature, so the embedder is a LOCAL static model
    # (model2vec — a distilled lookup table: pure CPU, no key, no GPU, ~30MB from HF on
    # first load). 'auto' = model2vec when importable, else the semantic door stays closed
    # and search runs its lexical doors only; 'none' forces it closed.
    osiris_embed_provider: str = "auto"
    osiris_embed_model: str = "minishlab/potion-base-8M"
    # Placeful satellite (cron Phase 6/7): this agent's id + the vantages it provides
    # (comma-separated). It claims dispatched collection jobs needing one of these.
    osiris_satellite_id: str = "satellite:local"
    osiris_satellite_vantages: str = ""
    # Alert delivery throttle (the 3am-false-alert guard). The durable `alerts` row is
    # ALWAYS written; only DELIVERY (the side-channel sink) is rate-capped: at most
    # `osiris_alert_max_per_window` deliveries per watch per `osiris_alert_window_secs`,
    # and never the same (watch,object) twice inside `osiris_alert_cooldown_secs`. Excess
    # rows are kept + logged (a digest count), never lost.
    osiris_alert_max_per_window: int = 20
    osiris_alert_window_secs: int = 3600
    osiris_alert_cooldown_secs: int = 86400
    # Delivery sink (D2): a watch with a webhook_url POSTs there; else, if OSIRIS_ALERT_EMAIL
    # is set it emails (needs OSIRIS_SMTP_HOST — absent => recorded-only + warn, never crash);
    # else the alert is logged. The durable /alerts row is the record regardless.
    osiris_alert_email: str = ""
    osiris_smtp_host: str = ""
    osiris_smtp_port: int = 587
    osiris_smtp_user: str = ""
    osiris_smtp_password: str = ""
    # Worker dead-man's-switch (D3): the worker heartbeats each cron tick; GET /health/worker
    # reports 'stale' if the last beat is older than this — a silently-dead tripwire becomes
    # visible instead of an invisible gap.
    osiris_worker_heartbeat_stale_secs: int = 120
    # The developer-persona heartbeat (pulse): comma-separated local repo paths the autonomic
    # loop senses + re-ingests on each tick. Empty => the pulse watches nothing (no-op).
    osiris_dev_repos: str = ""
    # The PERSISTENT MCP server (the fleet floodgate). `stdio` (default) = one server per
    # session (each agent spawns its own subprocess + pool — fine for one, exhausts PG at
    # fleet scale: N agents × the pool). `streamable-http` = ONE always-on server on
    # (host, port) that the whole fleet connects to over HTTP, sharing a SINGLE pool — so
    # connections stay bounded no matter how many agents link. The systemd `osiris-mcp` unit
    # runs the http mode; other projects point their .mcp.json at the URL.
    osiris_mcp_transport: str = "stdio"
    osiris_mcp_host: str = "127.0.0.1"
    osiris_mcp_port: int = 8790
    # The shared server's pool: ONE pool for the whole fleet (min_size stays 1 so it's cheap
    # idle; grows to this under concurrency). 20 << PG max_connections=100, vs the old
    # per-agent 10 × 56 = 560 that would have exhausted it.
    osiris_mcp_pool_size: int = 20
    # Session-sensing (the last unsensed source): path to the Claude Code projects root
    # (usually ~/.claude/projects) whose session transcripts the worker senses on a cron —
    # distill → redact → extract → DERIVED backfill of decisions/threads/obligations the
    # session forgot to write back. Empty => off. Forward-only: an unseen transcript starts
    # at its current end; history is `python -m src.ingest.sessions backfill`'s explicit job.
    osiris_sense_sessions: str = ""
    # The fleet TRIGGER-hook (mailbox → wake) — OFF by default. When on, the worker (the alarm
    # clock; never Osiris's own hands) spawns `claude -p` in a recipient project's repo when it
    # has unread mail, so an agent processes coordination without the operator hand-triggering it.
    # Recursion (the A↔B ping-pong) is bounded by a per-project rate cap: at most
    # osiris_trigger_rate_cap wakes per project per osiris_trigger_window_secs — each side of a
    # loop hits its own cap and halts. The agent_wakes ledger makes the chain visible; the enabled
    # flag is the kill switch (membrane #6: never silent, never irreversible).
    osiris_trigger_enabled: bool = False
    osiris_trigger_rate_cap: int = 5
    osiris_trigger_window_secs: int = 3600
    # Wake-GRACE (the double-wake guard, obligation c45bb2e3): the cron ticks (60s) faster than a
    # woken agent can spawn, mount, and lease its inbox (~100s+). In that gap the mail is still
    # deliverable, so a naive re-tick wakes a SECOND agent for the SAME message. A project woken
    # within osiris_trigger_grace_secs is skipped as 'wake-grace' (recently woken, still
    # processing) — distinct from 'rate-capped' (the loop bound); grace/lease expiry re-arm it.
    osiris_trigger_grace_secs: int = 300
    # Mail delivery LEASE (at-least-once, decision 56f6a0d6): inbox() leases a message rather
    # than consuming it; if no ack (or reply) settles it within this window, it REDELIVERS —
    # a response severed by a server bounce costs a duplicate, never a silent loss.
    osiris_mail_lease_secs: int = 900
    # Resume-not-mint (thread 9f2ddb44): the wake dispatch is deliver → resume → mint. A
    # transcript larger than this is at the context ceiling (retirement-by-compaction
    # territory) — NOT resumable; the wake mints a fresh twin instead. An owner whose mount
    # is fresher than osiris_owner_live_secs is LIVE: no wake at all, the mail just sits in
    # its box (the owner's own chrome/orient shows it — never spawn beside a live owner).
    osiris_resume_ceiling_bytes: int = 8_000_000
    osiris_owner_live_secs: int = 900
    # Wake ECONOMICS (operator, 2026-07-08): most wakes are triage-shaped (read, reply,
    # settle) — pin them to a cheaper model and let the PROMPT escalate real work back to a
    # full session (obligation + brief instead of grinding it in a haiku). Empty = the CLI's
    # default model (no --model flag passed).
    osiris_wake_model: str = ""
    # The operator's STANDING model choice (the intent). The fable harness silently demotes
    # fable→opus when it senses danger (ruling f2ae6346); the swap-detector flags an observed
    # model that diverges from this — the confession backstop the cold-boot ritual can't be.
    osiris_expected_model: str = "claude-fable-5"


def get_settings() -> Settings:
    return Settings()
