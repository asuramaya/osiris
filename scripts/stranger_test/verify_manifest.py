#!/usr/bin/env python3
"""Stranger-test proof A, manifest assertion: proves `osiris rename-project ... --apply`'s own
printed receipt actually named our chartered seat as cascaded — not merely that the CLI exited
0. `cmd_rename_project` (src/cli.py) prints the full `rename_project` receipt dict verbatim, one
`  key: value` line per top-level key (`print(f"  {k}: {v}")`, no json.dumps) — this reads that
captured stdout back and greps it with a targeted regex rather than `ast.literal_eval`-ing the
whole thing: the manifest's per-tier `detail` fields can carry non-literal reprs (nested receipt
dicts from `set_charter`/`correct_pin_value_third_party`/etc. — not guaranteed to be pure
literals all the way down), so a full-dict parse is the fragile path here, not the robust one.

The manifest shape (see `_cascade_governing_seats`, src/orchestrator/project_identity.py
~line 526): {"seats": {seat_canonical: {"pin": {...}, "house": {...}, "charter": {"status":
...}, "office": {...}, "tree": {...}}, ...}, "could_not_reach": {...}}. `could_not_reach` is a
FIXED, always-present pair of generic notes (the project's own on-disk folder; the repo root's
own .osiris file) — never a per-seat failure signal, so this checks the per-seat CHARTER tier
instead: the one tier a seat that charters the renamed project must show either "touched" (the
charter list itself got rewritten) or "already-correct", never "could-not". The regex anchors on
the seat's own canonical key first, then takes the FIRST 'charter': {'status': ...} that follows
it in the dict repr's own printed key order (pin, house, charter, office, tree per
`_cascade_governing_seats`'s own source order) — the first such match after the seat's own key is
guaranteed to be this seat's own charter tier, not some other seat's, because no other seat's
blob can intervene before this seat's own "charter" key appears.

Usage: verify_manifest.py <captured-stdout-file> <seat-canonical>
Exits 0 with a confirmation line on success; exits 1 naming exactly what's wrong otherwise."""
import re
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: verify_manifest.py <captured-stdout-file> <seat-canonical>",
              file=sys.stderr)
        return 2
    path, seat = sys.argv[1], sys.argv[2]
    text = open(path, encoding="utf-8").read()
    if "manifest:" not in text:
        print(f"NO manifest KEY FOUND in {path} — rename-project's own receipt print "
              "changed shape, or the command never printed a receipt at all", file=sys.stderr)
        return 1
    seat_key_idx = text.find(f"'{seat}'")
    if seat_key_idx == -1:
        seat_key_idx = text.find(f'"{seat}"')
    if seat_key_idx == -1:
        print(f"seat {seat!r} does not appear as a key in the manifest at all — the "
              "cascade never reached it", file=sys.stderr)
        return 1
    tail = text[seat_key_idx:]
    m = re.search(r"'charter':\s*\{'status':\s*'([\w-]+)'", tail)
    if m is None:
        m = re.search(r'"charter":\s*\{"status":\s*"([\w-]+)"', tail)
    if m is None:
        print(f"found seat {seat!r} in the manifest but no charter tier status followed "
              f"it — printed shape may have changed. context:\n{tail[:400]}", file=sys.stderr)
        return 1
    status = m.group(1)
    if status not in ("touched", "already-correct"):
        print(f"seat {seat!r}'s charter tier is {status!r} — expected 'touched' or "
              f"'already-correct'. context:\n{tail[:400]}", file=sys.stderr)
        return 1
    print(f"MANIFEST OK: seat {seat} charter tier = {status!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
