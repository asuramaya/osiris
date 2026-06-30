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
    # 'claude-cli' (the installed Claude Code, ideal for the core box) | 'anthropic' (an API
    # key, for satellites/remote with no CLI) | 'none'. `osiris_vision_model` OCRs a scanned
    # page → text before extraction (county notices are scans).
    osiris_extract_provider: str = "anthropic"
    osiris_claude_binary: str = "claude"
    osiris_vision_model: str = "claude-haiku-4-5-20251001"
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


def get_settings() -> Settings:
    return Settings()
