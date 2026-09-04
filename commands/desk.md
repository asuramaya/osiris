Read the operator's desk and render it. Call the osiris MCP tool `inbox` with project='operator' (the desk is ALWAYS returned peek-shaped and organized — reading it never leases; the count means "briefs the operator hasn't dismissed").

Render the organized shape in band order, compact:
1. **NEEDS YOUR DECISION** then **BLOCKED ON YOUR HANDS** then **FYI** — one block per card: sender project + agent, age, body. A card with `same_story` is one condition with several witnesses: render "×N also: <projects>" on one dim line (its `also` ids settle with the lead). A card with `thread_folded` supersedes earlier briefs in its thread: note "(supersedes N earlier)".
2. **DIMMED** — one line each: headline + who dimmed it + the moot reason. These are agent-annotated moot but still the operator's to dismiss.
3. **YOUR QUEUE** — the derived owner='operator' open threads, one line each (id + summary). This is the canonical waiting-on-your-hands list from the graph.

If everything is empty, say "desk clear".

SETTLE — only at the operator's word: if $ARGUMENTS contains "settle" or "clear" (or the operator explicitly says so), first render as above, then settle exactly the ids you rendered — including each fold's `also`/`thread_folded` ids and, if the operator says "clear dimmed", the dimmed ids — with inbox(project='operator', ack=[ids]) and confirm the new desk count. Scoped settles are honored: "settle the fyi band", "clear dimmed". Never settle on your own initiative; never settle ids you did not just show. Threads in YOUR QUEUE are never settled from here — they close via resolve_thread when the work is actually done.
