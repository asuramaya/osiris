"""THE `actions` FIXTURE HANDS EVERY TEST AN EMPTY OUTBOX. Seeding the Type catalog (once per
xdist worker, on whichever test draws the first seat) writes well over a thousand outbox
rows, and `evaluate_watches` claims its batch with `ORDER BY id LIMIT 500`, so a test that
inherited that noise saw its own events land past the batch and the watch evaluator tests
failed order-dependently: reliably when each test is the first on its worker (`-n 16` on a
small file), rarely in a full run. This pins the invariant directly, so a regression shows up
here with a plain name instead of as an intermittent evaluator failure."""
from __future__ import annotations

from src.actions.core import Actions


async def test_the_actions_fixture_starts_with_an_empty_outbox(actions: Actions) -> None:
    assert await actions.pool.fetchval("SELECT count(*) FROM outbox") == 0


async def test_the_catalog_is_seeded_but_left_no_outbox_noise(actions: Actions) -> None:
    from src.ontology.catalog import is_known_object_type

    assert await is_known_object_type(actions.pool, "Organization")
    assert await actions.pool.fetchval("SELECT count(*) FROM outbox") == 0
