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
    # THE DAILY CEILING (src/orchestrator/ceiling.py) — what Osiris may SPEND in a rolling 24h
    # before every paid producer stops. Until now NOTHING said this, anywhere, and every
    # catastrophe in this system's life was a spend catastrophe: a miner that walked every
    # transcript forever, a trigger that minted 463 real Claude sessions on abandoned projects.
    # The operator's law: "nobody will touch this if it burns."
    # Osiris's real measured spend, per day, over its whole life: median ~$3.60, peak $11.89.
    # So $10 is "a busy day and no worse" — it would never have blocked an honest day's work.
    # 0 = STOPPED (an honest kill switch). < 0 = UNLIMITED (a deliberate choice to run with no net).
    osiris_daily_usd: float = 10.0
    # THE COMMIT MINERS (pulse: mine_threads + mine_decisions) — DARK BY DEFAULT, by the operator's
    # ruling of 2026-07-14. They INFER that a sentence in a commit body is a durable duty or a
    # decision, and both failed the licence the rest of the fleet lives under (11.1% and 0%).
    # They survived every guard we built for one reason: THEY COST NOTHING, so the daily ceiling
    # could not see them and the miner kill-switch (which names `session-miner`) did not reach the
    # pulse daemon at all. The charter's line is OBSERVE vs INFER, not paid vs free — being free is
    # not a licence, it is only the reason nobody was watching. The pulse's OBSERVATIONS (repo
    # sensing, commit + tree ingest) are unaffected and always run: they cannot be wrong.
    osiris_mine_commits: bool = False
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
    # chrome (the read-only HTTP console, src/api/app.py) — a SEPARATE process/port from
    # osiris-mcp, run via `uvicorn --factory src.api.app:create_app` (deploy/osiris-console.
    # service). No prior settings field existed for this; it was a bare convention baked into
    # the uvicorn invocation. The smoke verb (thread bb763977/1849d800, task #63) is the first
    # caller that needs to know it as configuration rather than hardcoding the port.
    osiris_console_base_url: str = "http://127.0.0.1:8011"
    # Session-sensing (the last unsensed source): path to the Claude Code projects root
    # (usually ~/.claude/projects) whose session transcripts the worker senses on a cron —
    # distill → redact → extract → DERIVED backfill of decisions/threads/obligations the
    # session forgot to write back. Empty => off. Forward-only: an unseen transcript starts
    # at its current end; history is `python -m src.ingest.sessions backfill`'s explicit job.
    osiris_sense_sessions: str = ""
    # THE ADVERSARY'S SCOPE (task #37): which projects the sensing licence covers — a
    # comma/space list matched against transcript project-dir slugs by suffix ('pokex',
    # 'thoth', 'code/pokex'; see src/ingest/scope.py). Empty = every project (the unarmed
    # default; ships dark — arming it is the operator's hand). Scope DEFERS reading, never
    # buries it: scoped-out transcripts are never marked swept, so widening the scope later
    # lets the orphan reaper drain the interim backlog through the normal licensed lanes.
    osiris_sense_projects: str = ""
    # THE FREE OBSERVER, and it has its OWN switch on purpose. The transcripts root, read with a
    # `stat()` and nothing else: a session that is alive is WRITING TO ITS TRANSCRIPT whether or
    # not it is talking to us, so liveness is the freshest of (osiris call, transcript write).
    # DELIBERATELY SEPARATE from osiris_sense_sessions above, which is the ADVERSARY'S licence to
    # read those same files WITH A MODEL — that costs money and is gated; this costs nothing and
    # is always right. KILLING THE EXPENSIVE INFERRER MUST NEVER BLIND THE FREE OBSERVER. It is
    # the whole charter for a background critter: observe for nothing, infer only on a licence —
    # and never let them share a switch, or one day someone pulls the wrong one.
    osiris_transcripts: str = ""
    # THE DISK-CENSUS ROOTS (thread 5e37630b): colon-separated dirs the census walks for
    # git repos the graph has never met — 'exists on disk' becomes a first-class fact.
    # Rides the OBSERVER's switch (census runs only when osiris_transcripts is set): a
    # free deterministic disk read, the same class as the transcript sweep. Empty = the
    # operator's layout.
    osiris_census_roots: str = "~/code:~/code/REPOS"
    # The fleet TRIGGER-hook (mailbox → wake) — OFF by default. When on, the worker (the alarm
    # clock; never Osiris's own hands) spawns `claude -p` in a recipient project's repo when it
    # has unread mail, so an agent processes coordination without the operator hand-triggering it.
    # Recursion (the A↔B ping-pong) is bounded by a per-project rate cap: at most
    # osiris_trigger_rate_cap wakes per project per osiris_trigger_window_secs — each side of a
    # loop hits its own cap and halts. The agent_wakes ledger makes the chain visible; the enabled
    # flag is the kill switch (membrane #6: never silent, never irreversible).
    osiris_trigger_enabled: bool = False
    # THE RE-ARM SCOPE (the protocol every handoff since XXVII demanded: "turn it on for ONE
    # project, watched"): comma-separated project allowlist — when non-empty, only the named
    # projects may be woken; everything else is scoped_out. Empty = all projects (the
    # pre-scoping behavior). The trigger's own history is 463 mints on projects the operator
    # had not opened in days; a re-arm after a dark period should NAME ITS SUBJECTS, and this
    # makes that a setting instead of a promise.
    osiris_trigger_projects: str = ""
    # 15/hr per PAIR, not 5 (Thoth LIII 2026-07-21): 5 was measured-too-tight — active
    # manager<->worker collaboration is BURSTY (several knocks in minutes, then quiet), and a
    # 5/hr pair cap smothered legitimate dispatch (ruling bcaae418's evidence: two capped nudges
    # in ten minutes on the two most important messages of the day, both rescued by hand; the
    # fleet then SITS until a human nudges it). A true ping-pong runaway sustains a high rate
    # INDEFINITELY and is still caught at 15; a human-directed burst fits under it. The cap bounds
    # a loop, it must not throttle a conversation.
    osiris_trigger_rate_cap: int = 15
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
    # THE POKE'S IDLE GATE (the wake law, Phase 2): mail routed to a manager-owned window is
    # TYPED into it as its next turn — but never into a window whose output moved within this
    # many seconds (a streaming turn, or the echo of the operator typing). A busy window's
    # mail waits for the next tick; delivery (owner_live) covers the actively-working case.
    osiris_poke_min_idle_secs: int = 600
    # THE LEASE GATE'S REFUSAL (Alfred's field spec, msg 637 → task #22; thread fd921b7d).
    # OFF = today's advisory-only birth receipt. ON = a foreign body summoned into a Seat's
    # charter room while the resident lineage's pulse is live is REFUSED, not warned —
    # the failure class this kills is UNATTRIBUTED PRESENCE. The build is complete either
    # way; the flag exists because THE OPERATOR RATIFIES refusal semantics before they can
    # block anyone's hand (a lease that can block the owner needs the owner's word).
    osiris_lease_refuse: bool = False
    # THE POKE-ONLY ARM (operator ruling, 2026-07-19: 'arm the poke ... but dont turn on the
    # miners or critter background agents yet'): when true, the trigger's ladder ends at the
    # poke — deliver to a live owner, type into an open window, and NOTHING ELSE. No resume,
    # no mint, no new process on the operator's card, ever. Mail with no live owner and no
    # open window stays pull-only until the spawning rungs get their own re-arm. This is a
    # LANE switch, deliberately separate from osiris_trigger_enabled (the ladder's master)
    # and from the miner's licence — three different levers, three different costs.
    osiris_trigger_poke_only: bool = False
    # Wake HANDS (thread ba73c0c8): a triggered `claude -p` is headless — it cannot answer a
    # permission prompt, so in a repo with no stored approval every mcp__osiris__* call is
    # silently DENIED: the wake dies blind, its mail never settles, redelivers, re-wakes (the
    # 76-thread storm of 2026-07-11). The spawner must authorize the hands it asks for:
    # this comma/space list is passed as --allowedTools. `mcp__osiris` = every tool of the
    # osiris server and nothing else (a triage wake needs no Bash, no Edit). Empty = old
    # behavior (rely on the repo's stored approvals).
    osiris_wake_allowed_tools: str = "mcp__osiris"
    # Wake economics (obligation 4e52af7e): the fleet-wide hourly wake ceiling the trigger
    # reads — the same ledger the chrome displays as 'wakes N/h'. Past 80% of it, only
    # urgent mail (the operator's word, or mail aged past an hour) wakes; at it, nothing
    # does until the window slides. 0 = unmetered (the old behavior).
    osiris_wake_hourly_budget: int = 30
    # THE TOTAL, not a rate. How many times ONE message may wake a project before the trigger
    # gives up on it forever and escalates to the operator. Every other wake guard is a rate over
    # a sliding window, so every one of them RESETS — which is how a single unread letter spawned
    # 79 `claude -p` sessions in 18 hours on an abandoned project (2026-07-12). A retry that has
    # failed 79 times is not a retry; it is a leak.
    osiris_wake_message_attempts: int = 3
    # THE BACKGROUND-SESSION ADAPTER (ruling 6c4d0b62): the fleet runs as harness-backgrounded
    # sessions under one spawner pty — no pty fd to poke, no turn in flight to stop-hook — so
    # RESUME is the DM lane's primary push: a DM's arrival dispatches immediately (send()
    # itself dispatches; the worker tick is the backstop that drains queues), never a clock.
    # This arm is DELIBERATELY SEPARATE from osiris_trigger_poke_only: that switch holds the
    # BROADCAST spawn rungs (the operator's 2026-07-19 'no critter background agents' word,
    # which still stands for room mail); this one arms the DM resume the 2026-07-20 adapter
    # ruling made the push lane. Off = the DM lane is pull-only again.
    osiris_dm_resume: bool = True
    # MID-TURN, not "recently live": an addressee whose activity is fresher than this is
    # actually working RIGHT NOW — deliver, don't resume (its own turn's end surfaces the DM).
    # Distinct from osiris_owner_live_secs (the broadcast lane's 15-min liveness) because for
    # a backgrounded session "live 10 minutes ago" does NOT mean "perceiving": it idles with
    # no next turn coming, and mail beside it sits unread forever — the exact silent case the
    # adapter exists to close.
    osiris_dm_active_secs: int = 120
    # The per-seat mail-wake rate brake (anti-spiral wall #4 of 6c4d0b62): at most this many
    # wakes per ADDRESSEE per hour, on top of per-message dedup, the fleet hourly budget and
    # the daily dollar ceiling. An A<->B reply ping-pong is legal work until a brake says
    # otherwise; this is the brake that says it per seat. 0 = unbraked.
    osiris_seat_wake_hourly_cap: int = 6
    # Model for DM resumes. EMPTY ON PURPOSE (no --model flag): a DM resume continues a REAL
    # seat's own session — pinning the triage model onto it would be a silent model downgrade
    # of a working seat (the rug-pull class). The triage/mint lanes keep osiris_wake_model.
    osiris_dm_resume_model: str = ""
    # THE DEFAULT FLIP (task #68 wave, rulings 0fe36e59 + 33d6a2eb clause 3): launch_seat's
    # default spawn lane is the harness-native substrate (`claude --bg`, _spawn_claude_bg) —
    # every body it creates is visible in the operator's own `claude agents` list BY
    # CONSTRUCTION (clause 3, "front end wide open", made mechanical instead of patched
    # around). "pty" keeps the OLD osiris PTY-broker lane alive as an explicit, vendor-neutral
    # fallback (thread c171a3de) — for an incident, or a harness build that lacks --bg. A
    # launch_seat caller's own `substrate` argument always wins over this fleet-wide default.
    osiris_launch_substrate: str = "harness"
    # THE PAIR HEARTBEAT (Pit Watch Stage B, thread 449bf55d) — OFF by default, the same law
    # as osiris_trigger_enabled: a mechanism that pages the operator's desk earns its own kill
    # switch, never inherits one. When on, a tick alarms on any managed_by pair's ask-graded
    # DM sitting unread past osiris_mail_lease_secs while the addressee is provably not
    # mid-turn (_turn_fresh_sync); after osiris_pit_watch_escalate_at consecutive sightings,
    # ONE brief reaches the operator naming the pair and the message, then a tombstone stops
    # it from ever firing twice on the same message.
    osiris_pit_watch_enabled: bool = False
    osiris_pit_watch_escalate_at: int = 3
    # THE FLEET RECONCILE REAPER (task #59 phase 2, Thoth's gate DM 2042) — OFF by default,
    # the same law as osiris_trigger_enabled: a mechanism that WRITES to the graph on a
    # schedule earns its own kill switch, never inherits one. When on, a tick runs
    # fleet_reconcile.reconcile_execute(execute=True) — the exact same acting verb reachable
    # by hand, composing fold_agent/resolve_fold_candidate for the two bulk-act buckets and
    # a row-scoped mount drop for dead-project residue. leave_for_human rows are never
    # touched, by construction. Flipping this flag is a SECOND signature on top of a reviewed
    # diff — the code ships inert; a human decides separately when it may actually act.
    osiris_fleet_reconcile_enabled: bool = False
    # THE CLOSURE MINER'S CADENCE (Thoth DM 2679, following the deploy that made this
    # defensible) — OFF by default, the same law as osiris_fleet_reconcile_enabled: a
    # mechanism that WRITES to the graph on a schedule earns its own kill switch, never
    # inherits one. When on, a tick runs close_by_commits(dry_run=False) fleet-wide — the
    # exact same acting verb reachable by hand. Its blast radius is narrower than the
    # reaper's (only a commit LITERALLY naming a thread's own short id auto-closes;
    # everything else stays a rot_candidate for a human to confirm) but it still writes
    # unattended, so it gets the same second signature before it may act.
    osiris_closure_miner_enabled: bool = False
    # THE GATES-ARE-LAW ENFORCEMENT SWITCH (task #131 follow-up, Thoth DM 2890, operator
    # ruling 4ef68cfe) — OFF by default, same law as osiris_closure_miner_enabled, but the
    # ACTION here is a REFUSAL not a write: scripts/gate_hook.py always RUNS ruff/mypy/scoped-
    # pytest against a commit and always PRINTS what it found, whether this is on or off — the
    # switch controls only whether a failing gate can actually abort a `git commit` in this
    # shared, 4-agent-concurrent tree. Proven retroactively (decision pending, DM 2890's own
    # acceptance test) against db8e3e9..HEAD before this flips to True.
    osiris_gate_hook_enforce: bool = False
    # THE FROZEN LANE (wake(), thread 9f566244 / handoff 8f005905) — OFF by default, and this
    # QUARANTINE LIFTED (ruling 85fba696, operator 2026-07-29, superseding 482c3d0f). This was
    # dark because the house read the Claude daemon reply lane as a harness-level RCE (decisions
    # 635911f4, bd256380) and disclosed it. Anthropic reviewed that disclosure and deemed the
    # behavior INTENDED DESIGN — so the premise of the quarantine is withdrawn and the lane is
    # sanctioned to use. What the house measured is still TRUE and still matters: an injected
    # turn is stamped origin.kind='human' by the harness regardless of who actually wrote it,
    # which is exactly why wake() prefixes its own self-identifying provenance marker (it
    # refuses to hide behind that label) — keep that discipline, it is attribution honesty, not
    # a workaround. Still NOT a public API: an undocumented internal of someone else's product,
    # free to change without notice, which is why the injectable `nudge` seam in
    # trigger.trigger_mail_tick stays — operational insurance now, not legal cover.
    osiris_wake_enabled: bool = True
    # The operator's STANDING model choice (the intent). The fable harness silently demotes
    # fable→opus when it senses danger (ruling f2ae6346); the swap-detector flags an observed
    # model that diverges from this — the confession backstop the cold-boot ritual can't be.
    osiris_expected_model: str = "claude-fable-5"
    # THE AMBIENT SEAM WHISPER (alfred's pitch d80621a7 piece 1): above this context %, every
    # osiris tool response carries one `context` line; the ALARM tier stays context_lens.
    # ALARM_PCT (one authority). 0 disables the whisper entirely. Default 63 — exactly 2/3,
    # the operator's word (2026-07-21): the whisper starts when a third of the life remains.
    osiris_seam_whisper_pct: int = 63


def get_settings() -> Settings:
    return Settings()
