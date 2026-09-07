Declare a seat's charter — composes existing verbs (decisions 4a3858e9/87457dc1, ruling 1a5eaf98), never new orchestration. Parse `$ARGUMENTS`: `set <seat> --repos r1,r2 --because "..."` is the only shape. `--repos` is the WHOLE charter, replacing whatever was declared before, never an increment.

Try the `charter` MCP tool first (self-declaration); if it refuses because `<seat>` isn't your own, call `charter_for` instead (needs `--because`) — its own refusal names who may declare for whom, don't restate that logic here.

Never substitute a repo name that doesn't resolve to a real SoftwareProject for one that looks close — surface `set_charter`'s own per-name rejection and let the caller ingest/confirm the repo first (`/project create`), then re-declare. Report both the accepted set and any rejection; one bad name never sinks the whole charter.
