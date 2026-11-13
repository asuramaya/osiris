#!/bin/sh
# Fails fast (exit 1) unless the checkout it runs in is on branch `main` with its tracked files
# matching HEAD. A gate calls it when it starts and again when its test run finishes, so a run
# that something moved off main, or edited under it, can never be pushed:
#     scripts/assert_on_main.sh start && <tests> && scripts/assert_on_main.sh end && git push
# Usage: assert_on_main.sh [label] [expected-head-sha]   (the sha is optional: when given, HEAD
# must still be exactly that commit, so a mid-run commit or reset is caught as well)
label="${1:-check}"
expected="${2:-}"
branch="$(git symbolic-ref -q --short HEAD 2>/dev/null || true)"
if [ "$branch" != "main" ]; then
    echo "assert_on_main ($label): REFUSED, HEAD is '${branch:-detached}', not main. This run is tainted: do not push it." >&2
    exit 1
fi
if [ -n "$expected" ] && [ "$(git rev-parse HEAD)" != "$expected" ]; then
    echo "assert_on_main ($label): REFUSED, HEAD moved from $expected to $(git rev-parse HEAD) during the run." >&2
    exit 1
fi
if ! git diff --quiet HEAD -- 2>/dev/null; then
    echo "assert_on_main ($label): REFUSED, tracked files differ from HEAD (something edited the checkout). This run is tainted." >&2
    exit 1
fi
echo "assert_on_main ($label): on main at $(git rev-parse --short HEAD), tracked files clean"
