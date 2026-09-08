`/fleet prune`: call `agent(action='fleet_prune', execute=<True iff "execute"/"--execute" follows, else False>)`. Print `would_drop_transcripts`/`would_bind` (dry run) or `dropped_transcripts`/`bound` (executed), plus `reconcile_buckets_untouched` as one note line. Compact, no advice.

`/fleet` alone: show the fleet at a glance. Call `fleet`, render `tree` verbatim (● live / ○ historical), then `fleet_digest(hours=24)`, one summary line (agents, unresolved, swapped, conversations, operator_unread) + danger map if swapped. Compact, no advice.
