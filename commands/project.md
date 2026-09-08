Manage SoftwareProjects — composes existing verbs (osiris decisions 4a3858e9/87457dc1, operator ruling 1a5eaf98), never new orchestration. Parse `$ARGUMENTS`: the first word is the verb.

**create `<name> --because`** — composes the `project` MCP tool (`action='create'`).

**rename `<name> <new_name> --because`** — composes the `project` MCP tool (`action='rename'`). If it refuses (the new name collides with an existing project), surface the refusal verbatim — that guard is `project(action='rename')`'s own and is not this command's to work around.

**set-tag `<name> <TAG> --because`** — composes the `project` MCP tool (`action='set_tag'`). Surface a refusal verbatim, same as `rename`.

Deliberately a small door: for anything about a project's own STATE (who owns it, its arc, its decisions, its activity) use `/seat roster --repo <name>` or the `dossier` MCP tool directly, not a new command here — a small surface that composes what already exists beats a wider one that duplicates it.
