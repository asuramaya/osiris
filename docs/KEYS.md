<!-- topic: operations -->

# Keys: the soul-store encryption key and the restic password

Osiris holds two secrets at rest, both under the same custody mechanism and the same
`osiris <verb>-key <action>` command shape, but protecting two different things:

| Secret | Protects | Command |
|--------|----------|------|
| **soul key** | every transcript line ever ingested (`soul_lines`/`soul_lines_cold`), encrypted at rest | `osiris soul-key ...` |
| **restic password** | the restic repository's own encryption, for off-box backup targets | `osiris restic-key ...` |

Neither is ever a plaintext file on disk by default. Both are sealed as a systemd user
credential (`systemd-creds`) and only ever decrypted in memory, either by systemd itself
before the relevant process even starts, or on demand by the command line. Read this page
before you run `osiris soul-key init` for the first time. It explains what each command
does, what it refuses, and what to expect on screen.

## What the soul key protects

The soul store is an infinite-retention, byte-verbatim archive of every raw transcript line
any connected agent harness has ever written: two Postgres tables, `soul_lines` (one row
per line) and `soul_lines_cold` (a compacted, compressed fold of older lines). The soul key
encrypts the stored content column in each, using Fernet, a standard symmetric encryption
scheme. Even someone who can read the Postgres data directory directly cannot read a
transcript without this key.

The MCP server and the background worker each resolve the key once, at their own startup.
A missing key does not stop either service from starting: that is a deliberate, disclosed
choice (see "What happens with no key" below), not a silent gap.

## The custody ladder

`osiris soul-key init` picks a storage method automatically, in this order, unless you
override it with `--backend`:

1. **`host+tpm2`**: a `systemd-creds` credential bound to this machine's own host key and
   its TPM2 security chip, the strongest option. Only available once you have joined the
   `tss` Linux group (`sudo usermod -aG tss $USER`, then log out and back in). `init` prints
   that instruction when it applies, rather than running it for you.
2. **`host-cred`**: the same `systemd-creds` mechanism, bound to the host key alone, with no
   TPM2 involved. This is what a fresh machine gets by default.
3. **`file`**: a plain file on disk, with restrictive file permissions. Only chosen
   automatically on a machine with no `systemd-creds` support at all. You can also force it
   explicitly (`--backend file`) for a deployment that doesn't use systemd. This is the
   older, weaker shape. Prefer letting `init` choose automatically unless you have a
   specific reason not to.

Where the pieces actually live, for the default per-user deployment shape with no explicit
`--path`:

| File | What it is |
|------|-----------|
| `~/.config/credstore.encrypted/soul.key` | the sealed key itself (`host-cred`/`host+tpm2` backends) |
| `~/.config/osiris/soul.key` | the plaintext key (`file` backend only) |
| `~/.config/osiris/soul.key.meta.json` | records which backend sealed the key, never the key bytes |
| `~/.config/osiris/soul.key.legacy` | present only while a rotation is in progress (the old key, set aside) |
| `~/.config/osiris/soul.key.recovery.json` | the recovery data from a Security Key enrollment, once you have made one. Safe to back up: reading it needs your physical Security Key and its PIN |

`~/.config/osiris/soul.key` is a logical name used to derive the paths above. Under the
`host-cred`/`host+tpm2` methods, the key itself never lives at that path at all; it lives
sealed under `credstore.encrypted/`.

## First install

Run once, before either service's first start, in your own terminal, as whichever user the
services actually run as. For the standard per-user deployment shape this project ships,
that is simply your own login user, no elevated privileges needed:

```bash
osiris soul-key init
osiris soul-key enroll-recovery
```

`init` mints a fresh key, seals it under the automatically chosen method, and prints a
reminder that the MCP server and worker won't see the new key until they restart. Pass
`--restart` to have it restart them for you. It refuses outright if a key already exists at
the resolved path (`osiris soul-key rotate` is the command for replacing a live one, never
`init` again).

`enroll-recovery` links a discoverable credential on your plugged-in FIDO2 Security Key:
plug it in, run the command, and it prompts for your PIN and a physical touch. It writes the
recovery file listed above and refuses if one already exists already (re-enrolling is an
explicit remove-then-redo, never a silent overwrite). `osiris soul-key status` warns
whenever one or fewer recovery paths are enrolled. Losing the live key with zero recovery
paths makes every stored transcript permanently unreadable.

Both services already carry the configuration this needs, installed automatically by
`osiris deploy`. There is nothing to hand-edit here. The configuration tolerates a missing
credential at service start. This is exactly what makes the order above possible at all: if
the service refused to start without a key, you could never run `osiris soul-key init`
through the already-deployed console to create one in the first place.

## What happens with no key

If neither service has a key yet, they start anyway and log a clear warning explaining that
new transcript rows are being written unencrypted and naming the exact fix. Every new
transcript row written in this state is stored as plain text, not encrypted. The warning
fires once per process, not once per row, so a busy ingest loop doesn't flood the log, and
the same message names the exact fix. This is a deliberate choice: refusing to start at all
would create a chicken-and-egg problem, since minting the key normally means running the
command line through the already-running console, which needs the service up first. Run
`osiris soul-key init` and restart both services as soon as you see this warning. Nothing
about the unencrypted window is silent or hidden.

## Rotating the key

Rotation is two steps on purpose, because a live process can keep a key cached in its own
memory across a restart:

```bash
osiris soul-key rotate            # step 1: mint the new key, re-encrypt every row that exists right now
# restart the MCP server and worker so they pick up the new primary key
osiris soul-key rotate            # re-run (safe to repeat) to sweep any rows written between mint and restart
osiris soul-key rotate --finish   # step 2: destroy the old key, once nothing is left under it
```

The first `rotate` mints a fresh key under the same storage method the old one used, sets
the old key aside, and immediately re-encrypts every row that already exists in the
database onto the new key. Anything either process writes before its own restart still
encrypts under the old key, cached in its own memory. That's safe, since the old key stays
valid to decrypt until `--finish` runs, which is why the command tells you to restart both
services and re-run it. `--finish` refuses unless a fresh check comes back completely clean:
zero rows still under the old key, zero broken rows. Only then does it delete the old key
for good. Re-run `enroll-recovery` afterward too. The old recovery file still refers to the
old key and stops being useful once `--finish` runs.

## Recovering onto a new machine

If you are standing up a new machine and want to reuse an existing Security Key enrollment,
rather than starting a brand new key from scratch:

```bash
osiris soul-key recover
```

This reads the recovery file, prompts for the same Security Key's PIN and a touch, and
unwraps the key. Before trusting anything, it checks the recovered key's own fingerprint
against the one recorded in the recovery file. A mismatch refuses outright, since this is
treated as a real problem, not a retryable glitch. On success it seals the key under the new
machine's own host credential. It refuses if a key already exists at the target path:
`recover` is for a machine with none, and `rotate` is the command once you already have a
live key.

There is also a browser-based enrollment and recovery path, reached from the console's
Settings pane, in the Key section (see below). Both the command-line and browser paths are
explicitly noted in their own source code as not yet exercised against real Security Key
hardware. They are built and tested against the underlying standards and libraries, but a
genuine end-to-end enroll-and-recover cycle against your own physical key is still owed
before either is fully trusted.

### Why the browser flow must run at `http://localhost:8011`

The browser standard behind Security Key support only treats a page as secure over plain
HTTP for the literal hostname `localhost`. The address `127.0.0.1:8011` does not qualify,
even though it points at the same machine. The console has to be opened at a hostname
matching its own configuration, or the browser refuses the operation with an unclear error.
The console checks this itself before ever attempting the operation and shows a clear
message instead, naming the exact address to use.

If you ever see that message, simply reopen the console at `http://localhost:8011`. Same
server, same port. Only the hostname in the address bar changes.

## Everyday operation

```
osiris soul-key <status|init|rotate|restore-drill|enroll-recovery|recover> [flags]
```

| Action | Does |
|--------|------|
| `status` | storage method, key age, whether a rotation is in progress, enrolled recovery paths, and the number of legacy unencrypted rows. Never the key bytes |
| `init` | mint the first key (refuses if one already exists) |
| `enroll-recovery` | link the live key to a FIDO2 Security Key (PIN and touch). Command-line only, never over the network API |
| `recover` | restore a key from a Security Key enrollment onto a machine with no live key yet. Command-line only, never over the network API |
| `rotate` | mint a new key and re-encrypt every row onto it. `--finish` once the result is clean |
| `restore-drill` | prove that an off-box backup repository actually restores. Runs against every configured off-box backup URL, or one named target via `--repo-url` |

Flags: `--path P` overrides the automatically resolved key file location. `--backend
host-cred|host+tpm2|file` applies to `init`/`recover` only. `--owner USER` applies to `init`
only, for a deployment running as a system account with no natural file owner of its own.
`--restart` applies to `init` only, and restarts both services right after minting.
`--print-recovery` applies to `init`/`rotate`, and also prints the older one-time printed
secret, off by default now that Security Key recovery is the primary path. `--json` prints
one compact, machine-readable line for every action.

`enroll-recovery` and `recover` are deliberately command-line only, since both need your own
hands on a physical PIN and touch, right there in the terminal, and no action under
`soul-key` is ever exposed to an automated agent. Minting, rotating, or recovering this key
from an automated call is exactly what this design exists to refuse.
`status`/`init`/`rotate`/`restore-drill` are also exposed through the console's own network
routes, since the console only ever listens on the local machine.

### Migrating old plaintext rows

If you are upgrading a machine that already has soul-store data from before encryption was
added, `osiris soul-key status` reports the number of legacy unencrypted rows. Run the
migration itself with:

```bash
uv run python scripts/osiris_encrypt_soul_lines.py         # dry run
uv run python scripts/osiris_encrypt_soul_lines.py --apply  # actually re-write the rows
```

This runs in small batches and is safe to re-run in the middle of a deploy against a process
that is still actively writing new rows.

## The restic password

`osiris restic-key <status|init> [flags]` manages the second credential under the exact same
storage ladder (`host+tpm2`/`host-cred`/`file`, defaulting to
`~/.config/credstore.encrypted/restic.password`), but it protects something entirely
different: the restic repository's own encryption for off-box backup targets (see
[BACKUP.md](BACKUP.md)), not the soul store. It is a completely separate secret. Losing or
rotating one never touches the other.

This is a deliberately smaller command than `soul-key`: only `status` and `init` exist
today. There is no `rotate`/`enroll-recovery`/`recover` yet. Rotating a restic password also
needs a separate pass against the live repository itself, which is future work and not yet
built.

```bash
osiris restic-key init      # mint the password (refuses if one already exists)
osiris restic-key status    # storage method and presence facts, never the password itself
```

Flags: `--path P` overrides the default `~/.config/osiris/restic.password`. `--backend
host-cred|host+tpm2|file` applies to `init` only. `--json` prints one compact line.

## The Settings pane (Key section)

The console's Settings pane, opened from the command palette or the gear icon in the
header, has a Key section showing your soul key's live status: whether one is present, its
storage method, path, age, and recovery paths, with buttons for Init, Rotate, Restore-drill,
and the two Security Key actions described above. "Enroll recovery (this browser)" only
appears when a key exists with zero recovery paths enrolled. "Recover (this browser)" only
appears when no key exists yet. See [REFERENCE.md](REFERENCE.md#the-consoles-settings-pane)
for the full pane layout and every section, and [DEPLOY.md](DEPLOY.md) (the "Soul-store
encryption" section) for how this custody mechanism fits into the wider deployment picture.
