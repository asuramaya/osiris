<!-- topic: operations -->

# Keys — the soul-store encryption key and the restic password

Osiris holds two secrets at rest, both under the same custody mechanism and the same
`osiris <verb>-key <action>` shape, but protecting two different things:

| Secret | Protects | Door |
|--------|----------|------|
| **soul key** | every transcript line ever ingested (`soul_lines`/`soul_lines_cold`), encrypted at rest | `osiris soul-key ...` |
| **restic password** | the restic repository's own encryption, for off-box backup targets | `osiris restic-key ...` |

Neither is ever a plaintext file on disk by default. Both are sealed as a **systemd user
credential** (`systemd-creds`) and only ever decrypted in memory — by systemd itself, before
the daemon process even starts, or on demand by the CLI. Read this page before you run
`osiris soul-key init` for the first time; it explains what each command does, what it
refuses, and what to expect on screen.

## What the soul key protects

The soul store is Osiris's own infinite-retention, byte-verbatim archive of every raw
transcript line any harness (Claude Code, DSH, …) has ever written — two Postgres tables,
`soul_lines` (one row per line) and `soul_lines_cold` (a compacted, gzip'd fold of older
lines). The soul key encrypts the `raw_line`/`content_gzip` column in each, using
**Fernet** (a standard symmetric scheme) — "one tier above host-disk trust": even someone
who can read the Postgres data directory cannot read a transcript without this key.

`osiris-mcp` and `osiris-worker` each resolve the key once, at their own boot. **A missing
key does not stop either service from starting** — it's a deliberate, disclosed trade-off
(see "What happens with no key" below), not a silent gap.

## The custody ladder

`osiris soul-key init` picks a backend automatically, in this order, unless you override it
with `--backend`:

1. **`host+tpm2`** — a `systemd-creds` credential bound to this machine's host key AND its
   TPM2 chip, the strongest option. Only available once you've joined the `tss` Linux group
   (`sudo usermod -aG tss $USER`, then log out and back in) — `init` prints that hint when
   it applies rather than running it for you.
2. **`host-cred`** — the same `systemd-creds` mechanism, bound to the host key alone (no
   TPM2). This is what a fresh box gets by default.
3. **`file`** — a plain file on disk, `chmod 0600`. Only chosen automatically on a box with
   no `systemd-creds` support at all; you can also force it explicitly (`--backend file`)
   for a non-systemd deployment. This is the old, weaker shape — prefer letting `init`
   auto-select unless you have a specific reason not to.

Where the pieces actually live (default `--user` deploy shape, no explicit `--path`):

| File | What it is |
|------|-----------|
| `~/.config/credstore.encrypted/soul.key` | the sealed key itself (`host-cred`/`host+tpm2` backends) |
| `~/.config/osiris/soul.key` | the plaintext key (`file` backend only) |
| `~/.config/osiris/soul.key.meta.json` | records which backend sealed the key — never the key bytes |
| `~/.config/osiris/soul.key.legacy` | present only while a `rotate` is in flight (the old key, parked) |
| `~/.config/osiris/soul.key.recovery.json` | the FIDO2 recovery blob, once you've enrolled one — safe to back up, since reading it needs your physical Security Key **and** its PIN |

`~/.config/osiris/soul.key` is a *logical name* used to derive the sidecar paths above —
under the `host-cred`/`host+tpm2` backends the key itself never lives at that path at all;
it lives sealed under `credstore.encrypted/`.

## First install

Run once, before either unit's first start, in your own terminal, as whichever user the
units actually run as (for the systemd `--user` shape this repo ships, that's simply your
own login user — no `sudo`):

```bash
osiris soul-key init
osiris soul-key enroll-recovery
```

`init` mints a fresh key, seals it under the auto-selected backend, and prints a restart
hint (`the key is minted, but osiris-mcp/osiris-worker won't see it until restarted — run
systemctl --user restart osiris-mcp.service osiris-worker.service`) — pass `--restart` to
have it do that restart for you. It refuses outright if a key already exists at the
resolved path (`osiris soul-key rotate` is the door for replacing a live one, never `init`
again).

`enroll-recovery` wraps the live key with a FIDO2 Security Key: plug it in, run the
command, and it prompts for your PIN and a physical touch. It writes the recovery blob
listed above and refuses if one already exists (re-enrolling is an explicit remove-then-
redo, never a silent overwrite). `osiris soul-key status` warns whenever one or fewer
recovery paths are enrolled — losing the live key with zero recovery paths makes every
stored transcript permanently unreadable.

**Both units already carry the wiring this needs** — `ImportCredential=soul.key` in
`deploy/user/osiris-mcp.service` and `osiris-worker.service`, installed automatically by
`osiris deploy`. There is nothing to hand-edit here. Unlike the older `LoadCredentialEncrypted=`
shape this replaced, `ImportCredential=` tolerates a *missing* credential at unit start —
which is exactly what makes the boot-before-init order above possible at all: if the unit
refused to start without a key, you could never run `osiris soul-key init` through the
already-deployed console to create one in the first place.

## What happens with no key

If neither unit has a key yet, they **start anyway** and log a loud warning:

```
osiris-mcp starting WITHOUT a soul-store encryption key — new soul_lines/soul_lines_cold
rows write as legacy plaintext until this is fixed: no soul-store encryption key found at
<path> (and OSIRIS_SOUL_KEY is unset) — run `osiris soul-key init` ONCE, in your own
terminal, ...
```

Every new transcript row written in this state lands as **plaintext**, not encrypted — the
warning fires once per process (never once per row, so a busy ingest loop doesn't flood the
log) and the same message names the exact fix. This is a deliberate policy: refusing to
boot at all would recreate a bootstrap deadlock (minting the key normally means running the
CLI through the already-running console, which needs the service up first). Run `osiris
soul-key init` and restart both units as soon as you see this warning; nothing about the
degraded window is silent or hidden.

## Rotating the key

Rotation is two steps on purpose, because a live daemon can keep a key cached in its own
process memory across a restart window:

```bash
osiris soul-key rotate            # step 1: mint the new key, re-wrap every row that exists right now
# ... restart osiris-mcp and osiris-worker so they pick up the new primary key ...
osiris soul-key rotate            # re-run (idempotent) to sweep any rows written between mint and restart
osiris soul-key rotate --finish   # step 2: destroy the old key, once nothing is left under it
```

The first `rotate` mints a fresh key under the same backend the old one used, parks the old
key at the `.legacy` sidecar path, and immediately re-wraps every row that already exists
in the database onto the new key. Anything either daemon writes **before** its own restart
still encrypts under the old key in its cached memory — safe (the old key stays valid to
*decrypt* until `--finish`), which is why the command tells you to restart both units and
re-run it. `--finish` refuses unless a fresh dry-run census comes back completely clean (zero
rows still under the old key, zero broken rows) — only then does it delete the old key for
good. Re-run `enroll-recovery` afterward too; the old recovery blob still wraps the old key
and stops being useful once `--finish` runs.

## Recovering onto a new machine

If you're standing up a fresh box and want to reuse an existing FIDO2 recovery enrollment
rather than starting a brand-new key from scratch:

```bash
osiris soul-key recover
```

Reads the recovery blob, prompts for the same Security Key's PIN and a touch, unwraps the
key, and — before trusting anything — checks the recovered bytes' own fingerprint against
the one recorded in the blob. A mismatch refuses outright ("this is a real break, not a
retryable glitch") rather than silently sealing a possibly-tampered key. On success it
re-seals the key under the *new* box's own host credential. Refuses if a key already exists
at the target path — `recover` is for a box with none; `rotate` is the door once you
already have a live key.

There is also a **browser-based** enroll/recover path, reached from the console's Settings
pane (Key section) — see [Settings pane](#the-settings-pane-key-section) below. Both the
CLI FIDO2 code path and the browser path are explicitly flagged in their own source as
**not yet exercised against real Security Key hardware** — built and unit-tested against
the underlying WebAuthn/`python-fido2` APIs, but a genuine end-to-end enroll+recover cycle
against your own physical key is still owed before either is fully trusted.

### Why the browser flow must run at `http://localhost:8011`

WebAuthn (the browser API behind FIDO2) only treats a page as a secure context over plain
HTTP for the literal hostname `localhost` — `127.0.0.1:8011` does not qualify, even though
it's the same machine. The console's own `rp_id` setting (`soul_key.rp_id`, default
`"localhost"`) has to match the page's own hostname exactly, or the browser refuses the
ceremony with an opaque `SecurityError`. The console checks this itself before ever calling
into WebAuthn and shows a clear message instead:

```
This console is served from '127.0.0.1', not 'localhost' — open the console as
http://localhost:8011 ...
```

If you ever see that, just re-open the console at `http://localhost:8011` — same server,
same port, only the hostname in the URL bar changes.

## Everyday operation

```
osiris soul-key <status|init|rotate|restore-drill|enroll-recovery|recover> [flags]
```

| Action | Does |
|--------|------|
| `status` | backend, key age, whether a rotation is in flight, enrolled recovery paths, and the live legacy-plaintext row count — **never the key bytes** |
| `init` | mint the first key (refuses if one already exists) |
| `enroll-recovery` | wrap the live key with a FIDO2 Security Key (PIN + touch); CLI-only, never over the API |
| `recover` | restore a key from a FIDO2 recovery enrollment onto a box with no live key yet; CLI-only, never over the API |
| `rotate` | mint a new key and re-wrap every row onto it; `--finish` once the receipt is clean |
| `restore-drill` | prove an off-box backup repository actually restores — every URL in `backup.offload_targets`/`backup.offbox_repositories`, or one via `--repo-url` |

Flags: `--path P` (override the auto-resolved key file location), `--backend
host-cred\|host+tpm2\|file` (`init`/`recover` only), `--owner USER` (`init` only, for a
root-run system-unit deploy with no natural file owner of its own), `--restart` (`init`
only, restart both units right after minting), `--print-recovery` (`init`/`rotate`: also
print the old-style one-time printed-secret banner — off by default now that FIDO2
`enroll-recovery` is the primary recovery path), `--json` (one compact machine-readable
line on every action).

`enroll-recovery` and `recover` are deliberately **CLI-only** — both need your own hands on
a physical PIN and touch, right there in the terminal — and no action under `soul-key` is
ever exposed as an MCP tool; minting, rotating, or recovering this key from an agent call
is exactly the shape this door exists to refuse. `status`/`init`/`rotate`/`restore-drill`
are also exposed as the console's own REST doors (`GET /soul-key/status`, `POST
/soul-key/init|rotate|restore-drill`) — the operator's own surface, since the console binds
to `localhost` only.

### Migrating old plaintext rows

If you're upgrading a box that already has soul-store data from before this encryption
build landed, `osiris soul-key status` reports the live legacy-plaintext row count. Run the
migration itself with:

```bash
uv run python scripts/osiris_encrypt_soul_lines.py         # dry run
uv run python scripts/osiris_encrypt_soul_lines.py --apply  # actually re-write the rows
```

Batched and safe to re-run mid-deploy against a daemon that's still actively ingesting.

## The restic password

`osiris restic-key <status|init> [flags]` — the **second** credential under the exact same
ladder (`host+tpm2`/`host-cred`/`file`, `~/.config/credstore.encrypted/restic.password` by
default), but protecting something entirely different: **the restic repository's own
encryption** for off-box backup targets (see [BACKUP.md](BACKUP.md)), not the soul store.
It's a completely separate secret — losing or rotating one never touches the other.

This is a deliberately smaller door than `soul-key`: only `status` and `init` exist today.
There is no `rotate`/`enroll-recovery`/`recover` yet — rotating a restic password also
needs a `restic key add`/`restic key remove` pass against the live repository itself, which
is future work, not shipped.

```bash
osiris restic-key init      # mint the password (refuses if one already exists)
osiris restic-key status    # backend + presence facts — never the password itself
```

Flags: `--path P` (override the default `~/.config/osiris/restic.password`), `--backend
host-cred\|host+tpm2\|file` (`init` only), `--json`.

## The Settings pane (Key section)

The console's Settings pane (`Ctrl+K` → *Settings*, or the gear icon in the header) has a
**Key** section showing your soul key's live status — present/backend/path/age/recovery
paths — with buttons for Init, Rotate, Restore-drill, and the two FIDO2 actions described
above (**Enroll recovery (this browser)**, shown only when a key exists with zero recovery
paths enrolled; **Recover (this browser)**, shown only when no key exists yet). See
[REFERENCE.md](REFERENCE.md#the-consoles-settings-pane) for the full pane layout and every
section, and [DEPLOY.md](DEPLOY.md) (the "Soul-store encryption" section) for how this
custody mechanism fits into the wider deploy picture.
