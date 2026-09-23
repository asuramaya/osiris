from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. Single-operator self-hosted box; secrets via env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://osiris:osiris@127.0.0.1:5432/osiris"
    redis_url: str = "redis://127.0.0.1:6379/0"
    # Single static operator identity, fills the actor role an SSO layer would provide.
    osiris_actor: str = "analyst:operator"
    osiris_artifact_dir: str = "./artifacts"
    # On-chain ingest. Etherscan v2 is the one base where "keyless" bends: the API
    # rejects unkeyed calls, but a free key lifts the whole limit. Empty means the ETH
    # connector degrades gracefully (returns an error dict, never crashes a run).
    etherscan_api_key: str = ""
    # The watch (cron phase 3): comma-separated query terms to watch for new SEC Form D
    # filings (e.g. "Neuralink,Anthropic"). Empty means the worker registers no source tick
    # (the watch stays source-agnostic until an operator names a beat).
    osiris_watch_form_d: str = ""
    # AI extraction (cron phase 4): the model used by the universal extractor. A
    # document-to-entities task is flash-tier; a larger model would be wasteful per filing.
    osiris_extract_model: str = "claude-haiku-4-5-20251001"
    anthropic_api_key: str = ""
    # The daily spend ceiling (src/orchestrator/ceiling.py): what Osiris may spend in a
    # rolling 24h window before every paid producer stops. Real measured spend, per day,
    # over the system's whole life: median ~$3.60, peak $11.89. $10 covers a busy day and
    # no worse; it would never have blocked an honest day's work.
    # 0 = stopped (an honest kill switch). Below 0 = unlimited (a deliberate choice to run
    # with no net).
    osiris_daily_usd: float = 10.0
    # The commit miners (pulse: mine_threads + mine_decisions) are dark by default. They
    # infer that a sentence in a commit body is a durable duty or a decision, and both
    # failed the quality bar the rest of the fleet lives under. They cost nothing to run,
    # so the daily ceiling could not see them and a cost-based kill switch did not reach
    # the pulse daemon at all. The rule is observe versus infer, not paid versus free;
    # being free is not a license, it is only the reason nobody was watching. The pulse's
    # observations (repo sensing, commit and tree ingest) are unaffected and always run:
    # they cannot be wrong.
    osiris_mine_commits: bool = False
    # Inference providers, the GPU-as-an-API-key abstraction (src/ingest/providers.py).
    # The engine never runs a GPU; the model is a hosted API (a key), the local claude CLI
    # (subscription-covered, no key), or a local GPU backend. `osiris_extract_provider`:
    # 'auto' (the default, prefer the local claude CLI if installed, else an API key) |
    # 'claude-cli' (force the installed Claude Code, no key) | 'anthropic' (force an API
    # key, for satellites/remote with no CLI) | 'none'. So the primary box works keyless
    # off its own Claude install, while a satellite uses its key. `osiris_vision_model`
    # OCRs a scanned page to text before extraction (county notices are scans).
    osiris_extract_provider: str = "auto"
    osiris_claude_binary: str = "claude"
    # The harness process adapter: which harness src/orchestrator/harness_process.py's
    # ProcessAdapter selects for spawn/resume/reply/list_sessions/stop. Same 'auto'
    # pin-or-environment shape as osiris_extract_provider above: pydantic-settings already
    # reads the OSIRIS_HARNESS_ADAPTER env var into this field by name, so a caller need
    # only read `settings.osiris_harness_adapter`. 'auto' (default) picks the first
    # available() adapter in [claude, dsh, crush, cursor] order; naming one forces it even
    # when unavailable (every door then refuses by name, never silently falls through to a
    # different adapter than the one named).
    osiris_harness_adapter: str = "auto"
    osiris_crush_binary: str = "crush"
    # The DSH opt-in profile: dsh's own launcher (`dsh --profile <name> [args...]`)
    # forwards everything after its own flags verbatim to whatever profile-specific app
    # boots; it never itself parses `--resume`, and "headless"/"tui" in its own --help
    # text are illustrative example profile names, not a guaranteed universal contract
    # (this box's real profiles directory carries only "web", confirmed live, not
    # assumed). Empty by default: the DSH adapter keeps refusing spawn/resume by name
    # until an operator who has a real profile configured for headless task execution
    # names it here, never a hardcoded guess. `osiris_dsh_profile` names that profile;
    # `osiris_dsh_resume_flag` is the exact flag that profile's own app understands for
    # continuing a session (its value is forwarded verbatim, never invented; a different
    # profile app may spell this differently, or not support it at all).
    osiris_dsh_profile: str = ""
    osiris_dsh_resume_flag: str = "--resume"
    osiris_vision_model: str = "claude-haiku-4-5-20251001"
    # Semantic search. The Claude CLI has no embeddings endpoint and keyless is a feature,
    # so the embedder is a local static model (model2vec, a distilled lookup table: pure
    # CPU, no key, no GPU, roughly 30MB from Hugging Face on first load). 'auto' = use
    # model2vec when importable, else the semantic door stays closed and search runs its
    # lexical doors only; 'none' forces it closed.
    osiris_embed_provider: str = "auto"
    osiris_embed_model: str = "minishlab/potion-base-8M"
    # A remote satellite agent (cron phase 6/7): this agent's id and the vantages it
    # provides (comma-separated). It claims dispatched collection jobs needing one of
    # these.
    osiris_satellite_id: str = "satellite:local"
    osiris_satellite_vantages: str = ""
    # Alert delivery throttle (the false-alert-at-3am guard). The durable `alerts` row is
    # always written; only delivery (the side-channel sink) is rate-capped: at most
    # `osiris_alert_max_per_window` deliveries per watch per `osiris_alert_window_secs`,
    # and never the same (watch, object) pair twice inside `osiris_alert_cooldown_secs`.
    # Excess rows are kept and logged (a digest count), never lost.
    osiris_alert_max_per_window: int = 20
    osiris_alert_window_secs: int = 3600
    osiris_alert_cooldown_secs: int = 86400
    # Delivery sink: a watch with a webhook_url posts there; else, if OSIRIS_ALERT_EMAIL
    # is set it emails (needs OSIRIS_SMTP_HOST; absent means recorded-only plus a warning,
    # never a crash); else the alert is logged. The durable alerts row is the record
    # regardless.
    osiris_alert_email: str = ""
    osiris_smtp_host: str = ""
    osiris_smtp_port: int = 587
    osiris_smtp_user: str = ""
    osiris_smtp_password: str = ""
    # Worker dead-man's-switch: the worker heartbeats each cron tick; GET /health/worker
    # reports 'stale' if the last beat is older than this, so a silently-dead worker
    # becomes visible instead of an invisible gap.
    osiris_worker_heartbeat_stale_secs: int = 120
    # The developer-persona heartbeat (pulse): comma-separated local repo paths the
    # autonomic loop senses and re-ingests on each tick. Empty means the pulse watches
    # nothing (no-op).
    osiris_dev_repos: str = ""
    # The persistent MCP server (the fleet's shared entry point). `stdio` (default) means
    # one server per session (each agent spawns its own subprocess and pool, fine for
    # one, exhausts Postgres at fleet scale: N agents times the pool). `streamable-http`
    # means one always-on server on (host, port) that the whole fleet connects to over
    # HTTP, sharing a single pool, so connections stay bounded no matter how many agents
    # link. The systemd `osiris-mcp` unit runs the http mode; other projects point their
    # .mcp.json at the URL.
    osiris_mcp_transport: str = "stdio"
    osiris_mcp_host: str = "127.0.0.1"
    osiris_mcp_port: int = 8790
    # The one real arq worker: unlike osiris_mcp_transport above, arq_worker.startup() had
    # no signal distinguishing the one systemd-managed worker from any ad hoc local `arq`
    # invocation (a stray worktree inheriting the shared DATABASE_URL and running the
    # worker line by hand) -- every such boot confessed truthfully but uselessly against
    # the real graph. Mirrors osiris_mcp_transport's own non-inferred shape exactly:
    # "primary" set only in the real osiris-worker.service unit; empty (the default)
    # everywhere else, including every local, manual, or test invocation.
    osiris_worker_role: str = ""
    # The shared server's pool: one pool for the whole fleet (min_size stays 1 so it's
    # cheap idle; grows to this under concurrency), well under Postgres's max_connections
    # of 100, versus a much larger old per-agent total that would have exhausted it.
    # A shrunken Postgres shared_buffers configuration makes every idle backend's memory
    # cost more relatively, and this daemon's own concurrency never measured near its old
    # ceiling the way the worker's did, so the pool size was lowered accordingly.
    osiris_mcp_pool_size: int = 8
    # The other two long-running daemons: osiris-worker and the console (src/api/app.py)
    # each called create_pool with no size override, silently inheriting the bare
    # asyncpg.create_pool default (max_size=10), unconfigured rather than a deliberate
    # bound. Named separately from osiris_mcp_pool_size (not one shared default) because
    # the two daemons' own concurrency shapes differ: the worker runs many concurrent
    # cascade and cron jobs, the console serves read-only HTTP requests one at a time.
    # The worker pool was raised once after a real hour-long measurement found it was the
    # only daemon peaking near its own ceiling under load, then lowered again after a
    # separate boot-spike measurement found many idle backends at boot, each carrying a
    # real Postgres shared-buffer memory cost independent of peak concurrency. Applied
    # from the idle-backend measurement pending further review, since the two
    # measurements answer different questions (peak concurrency under load versus
    # idle-backend memory at boot).
    osiris_worker_pool_size: int = 4
    osiris_api_pool_size: int = 10
    osiris_manager_pool_size: int = 10
    # The read-only HTTP console (src/api/app.py): a separate process and port from
    # osiris-mcp, run via `uvicorn --factory src.api.app:create_app` (the
    # osiris-console.service unit). No prior settings field existed for this; it was a
    # bare convention baked into the uvicorn invocation before the smoke check needed to
    # know it as configuration rather than hardcoding the port.
    osiris_console_base_url: str = "http://127.0.0.1:8011"
    # Session sensing (the last unsensed source): path to the Claude Code projects root
    # (usually ~/.claude/projects) whose session transcripts the worker senses on a cron:
    # distill, redact, extract, then a derived backfill of decisions/threads/obligations
    # the session forgot to write back. Empty means off. Forward-only: an unseen
    # transcript starts at its current end; history is a separate explicit backfill job.
    osiris_sense_sessions: str = ""
    # The sensing scope: which projects the sensing license covers, a comma/space list
    # matched against transcript project-dir slugs by suffix (see src/ingest/scope.py).
    # Empty means every project (the unarmed default; ships dark, arming it is the
    # operator's hand). Scope defers reading, never buries it: scoped-out transcripts are
    # never marked swept, so widening the scope later lets the orphan reaper drain the
    # interim backlog through the normal licensed lanes.
    osiris_sense_projects: str = ""
    # The first miner: one generic abstention miner, lanes as data. Live by explicit
    # operator choice, not dark like the sensing license above; miners had stayed off
    # over trust and infrastructure concerns, resolved before this was armed.
    osiris_abstention_miner_enabled: bool = True
    # Per-lane off switch, same shape as osiris_sense_projects' own comma/space list: lane
    # object-type names (e.g. "Decision,Thread") to silence without touching the others,
    # so a bad lane's own candidate-pool query going wrong never costs the whole miner.
    osiris_abstention_miner_lanes_off: str = ""
    # The free observer, and it has its own switch on purpose. The transcripts root, read
    # with a `stat()` and nothing else: a session that is alive is writing to its
    # transcript whether or not it is talking to the graph, so liveness is the freshest of
    # (osiris call, transcript write). Deliberately separate from osiris_sense_sessions
    # above, which is the license to read those same files with a model, which costs
    # money and is gated; this costs nothing and is always right. Killing the expensive
    # inferrer must never blind the free observer: observe for nothing, infer only on a
    # license, and never let them share a switch, or one day someone pulls the wrong one.
    osiris_transcripts: str = ""
    # The disk-census roots: colon-separated directories the census walks for git repos
    # the graph has never met, so "exists on disk" becomes a first-class fact. Rides the
    # observer's switch (census runs only when osiris_transcripts is set): a free
    # deterministic disk read, the same class as the transcript sweep. Empty means the
    # operator's own layout.
    osiris_census_roots: str = "~/code:~/code/REPOS"
    # The fleet trigger hook (mailbox to wake): off by default. When on, the worker (an
    # alarm clock, never Osiris's own hands) spawns `claude -p` in a recipient project's
    # repo when it has unread mail, so an agent processes coordination without a human
    # hand-triggering it. Recursion (an A-to-B ping-pong) is bounded by a per-project rate
    # cap: at most osiris_trigger_rate_cap wakes per project per
    # osiris_trigger_window_secs; each side of a loop hits its own cap and halts. The
    # agent_wakes ledger makes the chain visible; the enabled flag is the kill switch
    # (never silent, never irreversible).
    osiris_trigger_enabled: bool = False
    # The re-arm scope: comma-separated project allowlist; when non-empty, only the named
    # projects may be woken, everything else is scoped out. Empty means all projects (the
    # pre-scoping behavior). The trigger's own history includes hundreds of spawns on
    # projects that had gone unopened for days, so a re-arm after a dark period should
    # name its subjects explicitly, and this makes that a setting instead of a promise.
    osiris_trigger_projects: str = ""
    # A measured rate cap per pair (raised from an earlier, too-tight value): active
    # manager-worker collaboration is bursty (several knocks in minutes, then quiet), and
    # too low a per-pair cap smothered legitimate dispatch, leaving the fleet waiting on a
    # human nudge. A true ping-pong runaway sustains a high rate indefinitely and is still
    # caught at this cap; a human-directed burst fits under it. The cap bounds a loop, it
    # must not throttle a conversation.
    osiris_trigger_rate_cap: int = 15
    osiris_trigger_window_secs: int = 3600
    # Wake grace (the double-wake guard): the cron ticks faster than a woken agent can
    # spawn, mount, and lease its inbox. In that gap the mail is still deliverable, so a
    # naive re-tick wakes a second agent for the same message. A project woken within
    # osiris_trigger_grace_secs is skipped as recently woken and still processing,
    # distinct from a rate cap (the loop bound); grace or lease expiry re-arms it.
    osiris_trigger_grace_secs: int = 300
    # Mail delivery lease (at-least-once): inbox() leases a message rather than consuming
    # it; if no ack or reply settles it within this window, it redelivers, so a response
    # severed by a server bounce costs a duplicate, never a silent loss.
    osiris_mail_lease_secs: int = 900
    # Resume-not-mint: the wake dispatch order is deliver, then resume, then mint. A
    # transcript larger than this is at the context ceiling, not resumable, so the wake
    # mints a fresh session instead. An owner whose mount is fresher than
    # osiris_owner_live_secs is live: no wake at all, the mail just sits in its box (the
    # owner's own status view shows it; never spawn beside a live owner). This field's
    # measurement basis was corrected twice after live testing: it originally checked raw
    # transcript file size, which measured the wrong thing (a resume only hydrates the
    # content since the last harness auto-compaction, a small fraction of the total
    # file), then again after a real specimen showed raw JSONL bytes are not context
    # tokens and a tail can be dominated by huge tool-output blobs fed to the model once
    # and never rehydrated by a resume. This field now keeps exactly one job: a
    # catastrophic-corruption sanity bound, a tail whose raw size is so implausibly large
    # that its shape alone suggests something is actually broken, refused regardless of
    # what the primary occupancy check says. The primary resumability ceiling is now the
    # last recorded assistant usage occupancy against the harness's own context window.
    osiris_resume_ceiling_bytes: int = 64_000_000
    # The minimum-tail floor, replacing an old compaction-count gate. The old gate asked
    # "has this lineage ever compacted?" (any compaction at all refused). Measured live on
    # a real transcript: many compactions, but a large amount of real work after the last
    # one, refused anyway, factually wrong about that transcript, not a policy
    # disagreement. A session that compacts once and then does many more turns closes with
    # its post-compaction context fully intact; a compaction-count gate excludes exactly
    # the sessions worth resuming. The replacement checks the same tail-bytes number the
    # ceiling above already checks against a minimum floor instead: a tail at or near zero
    # means the session closed at the compaction seam itself, genuinely nothing to resume
    # into. Default picked, not measured the way the old gate's larger study was: small
    # enough to pass any transcript with real post-boundary activity, large enough to
    # exclude a truly-empty tail. A one-constant change if evidence says otherwise.
    osiris_resume_min_tail_bytes: int = 200
    osiris_owner_live_secs: int = 900
    # Wake economics: most wakes are triage-shaped (read, reply, settle), so pin them to a
    # cheaper model and let the prompt escalate real work back to a full session (an
    # obligation and a brief instead of grinding it out on a small model). Empty means the
    # CLI's default model (no --model flag passed).
    osiris_wake_model: str = ""
    # The poke's idle gate: mail routed to a manager-owned window is typed into it as its
    # next turn, but never into a window whose output moved within this many seconds (a
    # streaming turn, or the echo of someone typing). A busy window's mail waits for the
    # next tick; delivery to a live owner covers the actively-working case.
    osiris_poke_min_idle_secs: int = 600
    # The lease gate's refusal. Off means today's advisory-only birth receipt. On means a
    # foreign body summoned into a seat's charter room while the resident lineage's pulse
    # is live is refused, not warned, closing the failure class of unattributed presence.
    # The build is complete either way; the flag exists because refusal semantics that can
    # block anyone's hand need an explicit ruling before they arm (a lease that can block
    # the owner needs the owner's word).
    osiris_lease_refuse: bool = False
    # The poke-only arm: when true, the trigger's ladder ends at the poke, deliver to a
    # live owner, type into an open window, and nothing else. No resume, no mint, no new
    # process, ever. Mail with no live owner and no open window stays pull-only until the
    # spawning rungs get their own re-arm. This is a lane switch, deliberately separate
    # from osiris_trigger_enabled (the ladder's master) and from the miner's license:
    # three different levers, three different costs.
    osiris_trigger_poke_only: bool = False
    # Wake hands: a triggered `claude -p` is headless, it cannot answer a permission
    # prompt, so in a repo with no stored approval every mcp__osiris__* call is silently
    # denied: the wake dies blind, its mail never settles, redelivers, and re-wakes (a
    # real incident, a storm of dozens of threads from exactly this gap). The spawner must
    # authorize the hands it asks for: this comma/space list is passed as --allowedTools.
    # `mcp__osiris` means every tool of the osiris server and nothing else (a triage wake
    # needs no shell, no file edit). Empty means old behavior (rely on the repo's stored
    # approvals).
    osiris_wake_allowed_tools: str = "mcp__osiris"
    # Wake economics: the fleet-wide hourly wake ceiling the trigger reads, the same
    # ledger the status view displays as wakes per hour. Past 80% of it, only urgent mail
    # (marked urgent, or mail aged past an hour) wakes; at it, nothing does until the
    # window slides. 0 means unmetered (the old behavior).
    osiris_wake_hourly_budget: int = 30
    # The total, not a rate: how many times one message may wake a project before the
    # trigger gives up on it forever and escalates to a human. Every other wake guard is a
    # rate over a sliding window, so every one of them resets, which is how a single
    # unread letter once spawned dozens of sessions in under a day on an abandoned
    # project. A retry that has failed that many times is not a retry, it is a leak.
    osiris_wake_message_attempts: int = 3
    # The background-session adapter: the fleet runs as harness-backgrounded sessions
    # under one spawner pty, no pty file descriptor to poke, no turn in flight to
    # stop-hook, so resume is the DM lane's primary push: a DM's arrival dispatches
    # immediately (send() itself dispatches; the worker tick is the backstop that drains
    # queues), never a clock. This arm is deliberately separate from
    # osiris_trigger_poke_only: that switch holds the broadcast spawn rungs; this one arms
    # the DM resume path. Off means the DM lane is pull-only again.
    osiris_dm_resume: bool = True
    # Mid-turn, not "recently live": an addressee whose activity is fresher than this is
    # actually working right now, so deliver, don't resume (its own turn's end surfaces
    # the DM). Distinct from osiris_owner_live_secs (the broadcast lane's longer liveness
    # window) because for a backgrounded session "live a few minutes ago" does not mean
    # "perceiving": it idles with no next turn coming, and mail beside it sits unread
    # forever, the exact silent case this setting exists to close.
    osiris_dm_active_secs: int = 120
    # The per-seat mail-wake rate brake: at most this many wakes per addressee per hour,
    # on top of per-message dedup, the fleet hourly budget, and the daily dollar ceiling.
    # An A-to-B reply ping-pong is legal work until a brake says otherwise; this is the
    # brake that says it per seat. 0 means unbraked.
    osiris_seat_wake_hourly_cap: int = 6
    # Model for DM resumes. Empty on purpose (no --model flag): a DM resume continues a
    # real seat's own session, so pinning the triage model onto it would be a silent model
    # downgrade of a working seat. The triage and mint lanes keep osiris_wake_model.
    osiris_dm_resume_model: str = ""
    # The default spawn lane: launch_seat's default is the harness-native substrate
    # (`claude --bg`), so every body it creates is visible in the operator's own agent
    # list by construction. "pty" keeps the old osiris PTY-broker lane alive as an
    # explicit, vendor-neutral fallback, for an incident, or a harness build that lacks
    # --bg. A launch_seat caller's own `substrate` argument always wins over this
    # fleet-wide default.
    osiris_launch_substrate: str = "harness"
    # Crash replay as a gate: off by default, the same law as osiris_trigger_enabled: a
    # mechanism that kills a live service earns its own kill switch, never inherits one.
    # When on, `osiris deploy` runs the chaos-replay check as an additional gate after its
    # own ordinary graceful restart.
    osiris_deploy_chaos_gate: bool = False
    # The full test suite on the merged tree. Two live incidents drove this: a branch's
    # own scoped gate was green, but the same suite failed several tests on main for a
    # missing-module error the branch's own deletion caused; separately, a clean git
    # auto-merge of two branches touching the same file was only proven correct by
    # re-running the suite on the merged result. Neither incident was a daemon-crash
    # gap; the chaos gate would not have caught either one. The commit-time gate's own
    # pytest run is deliberately scoped to each commit's own resolved test files (a full
    # run per commit is unshippable under multi-agent concurrency); nothing else runs the
    # full suite against what a merge actually produces. When on, `osiris deploy` runs the
    # full suite against the deploy target as an additional precondition gate, before the
    # chaos gate.
    #
    # Still off by default: not the same shape as the commit-time gate's own later
    # arming (that flag gates a cheap boolean refusal; this one, on, makes many
    # deploy-path tests that never inject their own fake full-suite gate start spawning a
    # real nested pytest subprocess apiece, a test-suite-architecture change, not a
    # policy one, and out of scope here). Two concurrency flakes this gate's own dry run
    # surfaced were root-caused and fixed: a query with no explicit ordering returned rows
    # in whatever order the query planner's physical scan happened to produce, usually
    # insertion order on a quiet box but never guaranteed, and a shared-container-under-
    # real-load plan legitimately returned rows out of order, which every downstream
    # group or table operation silently inherited as meaningful (fixed with an explicit
    # ORDER BY); and a PTY-broker test shared its wait bound with many other call sites in
    # the same file but did the most real round-trip work of any of them, so the shared
    # bound had the least margin under host contention that is normal fleet load, not a
    # hypothetical (fixed by widening only that call's own bound, not the shared default).
    # Neither was a mechanism this gate itself was ever expected to see; it flagged them
    # running its own baseline suite, which was doing its job. Arming this, at actual
    # deploy time, is a separate decision from shipping the code.
    osiris_deploy_full_suite_gate: bool = False
    # The pair heartbeat: off by default, the same law as osiris_trigger_enabled: a
    # mechanism that pages a human's desk earns its own kill switch, never inherits one.
    # When on, a tick alarms on any manager-worker pair's ask-graded DM sitting unread
    # past osiris_mail_lease_secs while the addressee is provably not mid-turn; after
    # osiris_pit_watch_escalate_at consecutive sightings, one brief reaches the operator's
    # desk naming the pair and the message, then a tombstone stops it from ever firing
    # twice on the same message.
    osiris_pit_watch_enabled: bool = False
    osiris_pit_watch_escalate_at: int = 3
    # The fleet reconcile reaper: off by default, the same law as osiris_trigger_enabled:
    # a mechanism that writes to the graph on a schedule earns its own kill switch. When
    # on, a tick runs the reconcile-and-execute verb, the exact same acting verb reachable
    # by hand, composing the fold and duplicate-resolution primitives for the two bulk-act
    # buckets, plus a row-scoped mount drop for dead-project residue. Rows a human has
    # flagged for manual review are never touched, by construction. Flipping this flag is
    # a second signature on top of a reviewed diff; the code ships inert, a human decides
    # separately when it may actually act.
    osiris_fleet_reconcile_enabled: bool = False
    # The closure miner's cadence: off by default, the same law as
    # osiris_fleet_reconcile_enabled: a mechanism that writes to the graph on a schedule
    # earns its own kill switch, never inherits one. When on, a tick runs the
    # commit-driven closure sweep fleet-wide, the exact same acting verb reachable by
    # hand. Its blast radius is narrower than the reaper's (only a commit literally naming
    # a thread's own short id auto-closes; everything else stays a candidate for a human
    # to confirm) but it still writes unattended, so it gets the same second signature
    # before it may act.
    osiris_closure_miner_enabled: bool = False
    # The phantom-heal sweep's own switch: the false-mint class must heal mechanically,
    # never by hand, the same law as the two above: a mechanism that writes to the graph
    # on a schedule earns its own kill switch. Off by default. When on, a periodic tick
    # folds only fresh, never-flagged zero-turn phantoms (the going-forward class this was
    # built for); a phantom already flagged but never fully unwound is reported via an
    # obligation, never auto-completed, regardless of this switch.
    osiris_phantom_heal_enabled: bool = False
    # The phantom/fold backlog reap's own switch: this class of cleanup should manage
    # itself rather than be babysat, the same law as every switch above: a mechanism that
    # writes to the graph on a schedule earns its own kill switch, never inherits one. Off
    # by default. When on, a periodic tick reinstates a false-mint generation only when an
    # independent census confirms a live body beyond the graph's own claim, and
    # invalidates a duplicate project-membership edge only when exactly one live target is
    # a non-active, non-merged project. Parallel-lives and half-healed phantom threads are
    # counted and surfaced, never acted on; the code's own standing behavior there is
    # unchanged by this.
    osiris_phantom_fold_reap_enabled: bool = False
    # The tree-ingest alarm's own switch: self-healing over manual bug-chasing, the same
    # law as the switches above: a mechanism that acts on a schedule earns its own kill
    # switch, never inherits one. Off by default. When on, a periodic tick discovers trees
    # fleet-wide and, for each tree its owning seat has never been alarmed about in the
    # last 24h, sends that seat a graded ask; it never ingests anything itself, ingesting
    # a project is the owning seat's own act, always a deliberate second call. A tree with
    # no governing seat is reported in the tick's return value, never mailed to no one.
    osiris_tree_ingest_alarm_enabled: bool = False
    # The landing audit's own switch: a prior gap found the audit's only caller had not
    # completed in days, blocked by an unrelated gate, meaning the check itself was
    # correct but its sole trigger was dark. Same law as the switches above: a mechanism
    # that writes to the graph on a schedule earns its own kill switch, never inherits
    # one. Off by default. When on, a periodic tick runs the landing audit fleet-wide,
    # minting one obligation per branch unmerged into the main line for 48h or more with
    # no open held-work claim naming it, and per graph text whose cited merge is provably
    # not an ancestor of the main line. Idempotent on summary text; never blocks a deploy,
    # never gates anything.
    osiris_landing_audit_enabled: bool = False
    # The obligation hygiene no-regrow rule's own switch: after 7 idle days, a nudge goes
    # to the obligation's own owner (or the operator, when the owner is a project name or
    # resolves to no live agent); after 7 more days of silence past that, a
    # stale-candidate marker plus a desk brief follows. Never auto-resolved at either
    # stage; this only nudges and surfaces, it never closes or reclassifies a thread on
    # its own authority. True by default, a deliberate, named exception to every switch
    # above's dark-by-default convention, per explicit operator instruction to ship it on.
    osiris_obligation_hygiene_enabled: bool = True
    # The no-regrow rule's own switch: a separate clock from
    # osiris_obligation_hygiene_enabled above, keyed off `stale_after` rather than
    # idle-since-last-touch: an open obligation thread with no annotation, owner change,
    # or resolution for 21+ days past its own stale_after reclassifies to kind='task'
    # (never resolved, never a status change), with a receipt on the owner's mail. Off by
    # default; no explicit "ship it on" instruction accompanied this one, unlike its named
    # exceptions above.
    osiris_no_regrow_enabled: bool = True
    # The retention heartbeat's own switch: a daily tick deletes published outbox rows and
    # audit_log rows older than 90 days, batched, and posts a desk receipt naming both
    # counts every run. True by default, same named exception as
    # osiris_obligation_hygiene_enabled above, shipped on by explicit request. Measured
    # live before this ran: hundreds of megabytes each in the outbox and audit_log
    # tables, neither ever pruned before this.
    osiris_retention_heartbeat_enabled: bool = True
    # The soul store's cold tier switch: memory gets tiers, not deletion. A daily tick
    # folds up to a bounded batch of sessions untouched for 30+ days into one compressed
    # cold-tier row each, deleting their per-line hot-tier rows; the read paths (resume,
    # verify, rematerialize) read through both tiers transparently. True by default, same
    # named exception as osiris_obligation_hygiene_enabled above, shipped on by explicit
    # request.
    osiris_soul_cold_tier_enabled: bool = True
    # The gates-are-law enforcement switch: same law as osiris_closure_miner_enabled, but
    # the action here is a refusal, not a write: the commit-time gate script always runs
    # lint/type/scoped-test checks against a commit and always prints what it found,
    # whether this is on or off; the switch controls only whether a failing gate can
    # actually abort a `git commit` in this shared, multi-agent-concurrent tree. Armed
    # true after both blockers cleared: a retroactive replay across recent history (the
    # last round honestly skip-free) and a forward-looking acceptance test on a real
    # staged change to a previously-troublesome module, plain pass, no skips.
    # Deliberately still inert in the only way that can refuse anyone's commit:
    # `core.hooksPath` is not configured to point at the shared hooks directory, by
    # design, so the flag can be on and observed with several agents live before the hook
    # path can actually block anybody. Wiring `core.hooksPath` is a separate, later act,
    # not this one.
    osiris_gate_hook_enforce: bool = True
    # Stage C, the turn-end practice-violation audit: off, deliberately disarmed, not "not
    # yet armed" like its siblings above. Measured live for five days after it shipped: a
    # double-digit flag count fleet-wide in a single 24h sample, most individually
    # verified against the flagging agent's own turn text, zero confirmed true positives.
    # A graph-wide search for even one confirmed true-positive catch across this
    # mechanism's entire deployment found none. Root cause is not a precision problem (a
    # real signal diluted by noise); the topical-overlap gate it used is not a topic
    # signal at all in a corpus where nearly every turn shares a handful of common words,
    # and it fires hardest on the most careful engineering prose, an inverse-quality
    # signal. Scoped: this flag governs Stage C only. The write-time contradiction check
    # and wake-arming are different mechanisms with different evidence and are untouched
    # by this flag. Re-arming this needs a demonstrated true-positive rate, not another
    # narrow suppressor for a fifth false-positive shape.
    osiris_stage_c_practice_check_enabled: bool = False
    # The frozen lane (wake()): off by default, and this quarantine has since been lifted.
    # This was dark because the earlier read of the harness daemon reply lane treated it
    # as a harness-level remote-execution risk and disclosed it as such; the harness
    # vendor reviewed that disclosure and confirmed the behavior was intended design, so
    # the premise of the quarantine was withdrawn and the lane is sanctioned to use. What
    # was measured is still true and still matters: an injected turn is stamped as
    # human-originated by the harness regardless of who actually wrote it, which is
    # exactly why wake() prefixes its own self-identifying marker, it refuses to hide
    # behind that label, and that discipline stays, it is attribution honesty, not a
    # workaround. Still not a public API: an undocumented internal of someone else's
    # product, free to change without notice, which is why the injectable nudge seam in
    # the trigger's own tick stays, operational insurance now, not legal cover.
    osiris_wake_enabled: bool = True
    # The standing model choice (the intent). The harness silently demotes the intended
    # model to a fallback when it senses danger; the swap detector flags an observed model
    # that diverges from this as the confession backstop a cold boot can't provide on its
    # own.
    osiris_expected_model: str = "claude-fable-5"
    # The ambient context-usage notice: above this context percentage, every osiris tool
    # response carries one context line; the alarm tier stays the single authority for
    # when action is required. 0 disables the notice entirely. Default 45: the alarm
    # thresholds moved higher over time, and this notice must sit a tier below the alarm
    # or it never fires as its own tier.
    osiris_seam_whisper_pct: int = 45
    # Memory diagnostics: osiris-mcp has been observed oscillating within its memory cap
    # and swapping on some incarnations, cause unmeasured since the last capacity change.
    # Python's tracemalloc itself costs real CPU and memory overhead while tracing, off by
    # default, the same law every other diagnostic or write mechanism in this file
    # follows, so it never runs silently in production; an operator flips it on for a
    # measurement window only. The first version of this diagnostic caused a live outage
    # the same night it shipped: an unbounded trace pinned the event loop and only a hard
    # kill signal stopped it. The diagnostic route is now a bounded window on its own (a
    # small frame count, a fixed hard time cap, an in-window memory tripwire, refuses a
    # second concurrent window, refuses to start over a fixed memory threshold); this flag
    # still gates it entirely dark by default, but the route itself can no longer run away
    # even while the flag is on.
    osiris_memory_diag_enabled: bool = False
    # The worker boot spike: many run_at_startup cron jobs used to fire concurrently in
    # the same few-second window, racing for CPU and memory during the single costliest
    # moment of the process's life, measured at a large memory and swap spike at full CPU
    # shortly after restart. This bounds that window: cron jobs serialize one at a time
    # behind a lock for this many seconds after boot, then run concurrently as normal for
    # the rest of the process's life, cheap once the burst has passed, since a scheduled
    # tick past this deadline never touches the lock at all.
    osiris_worker_boot_serialize_s: float = 90.0
    # Ports the same bounded-tracemalloc safety rails used for the memory diagnostic above
    # (a small frame count, a memory tripwire, a hard duration cap, self-terminating with
    # nobody polling) to the worker's own boot burst, gated off by default for the same
    # reason osiris_memory_diag_enabled is: tracing costs real overhead even bounded, so
    # it must never run silently. Unlike the console route this has no HTTP surface to
    # poll; it logs the top allocation sites once, at the end of the window.
    osiris_worker_boot_memtrace_enabled: bool = False
    # Miner budget knobs, promoted off a module's own bare constants so they can be
    # registered in the settings registry with an effect that takes hold on the next
    # tick (the proposal logic already calls get_settings() fresh on every invocation, so
    # a write here is genuinely live on the very next miner tick, no restart needed).
    # Values unchanged from their prior constants.
    osiris_miner_daily_budget_base: int = 5
    osiris_miner_new_pair_starter_budget: int = 1
    osiris_miner_zero_acceptance_window_days: int = 7
    # Layout knobs (product law: every action has a door): the heartbeat's own per-tick
    # batch size and cron cadence, both previously bare module constants. batch_size takes
    # effect on the next tick and is genuinely table-driven (the layout batch job reads it
    # via the live settings-service path, not the env-overlay path, which only covers
    # immediate-effect keys); tick_seconds takes effect only on the worker's next restart,
    # since arq's cron schedule is a static literal evaluated once at worker-settings
    # class-definition time, same as every other daemon-literal knob in this house.
    osiris_layout_batch_size: int = 1000
    osiris_layout_tick_seconds: int = 300


def get_settings() -> Settings:
    return Settings()
