"""How a reader picks THE current value of a property.

`current_assertions` keeps one current row per (object, name, SOURCE), so a property can have
several current rows at once (two sources, or a fold and the reinstatement that reversed it).
Every reader that wants a single value resolves them the same way:

    ORDER BY confidence DESC, observed_at DESC, id DESC

Highest confidence wins; among equals the newest observation; among equals the LATER WRITE, which
is `id` (a bigint from `assertions_id_seq`, so it grows in write order: the row written last is the
one a reader sees). Without the last term two rows with the same confidence and the same
`observed_at` (a fold and the mint that adopts its numeral share one `now`) came back in whatever
order the plan produced, and a live heir could read as a false mint.

One caveat: `id` is the order of the INSERT. A repeat of the same value from the same source only
bumps `observed_at` on its existing row (same `id`), so a bump can lose a tie to a row from
another source that carries the identical `observed_at`. That needs the same confidence and the
same microsecond on two sources and a different value, which only a batch written at one instant
produces; the readers then see the row inserted last.

The literal text is written out at each reader (a shared string would have to be spliced into
hundreds of queries, several of them f-strings); `tests/test_current_order.py` reads every SQL
string in src and scripts and fails when a reader orders by confidence and observed_at without the
`id DESC` tiebreak, and checks the claim above against the database."""
from __future__ import annotations

CURRENT_VALUE_ORDER = "confidence DESC, observed_at DESC, id DESC"
