# The "stranger's machine" — task #97, the standing acceptance criterion that a person who
# has never seen osiris, on a box that has never run it, can clone the public repo and reach
# a working install following ONLY docs/INSTALL.md. This image bakes in exactly the four
# things INSTALL.md's "0. Prerequisites" section says the stranger already has (Linux,
# Python-capable, uv, Docker) — nothing osiris-specific. Everything from `git clone` onward
# happens at container-run time, in run.sh, replaying the doc verbatim.
#
# Docker-in-Docker (not a host-socket mount): the nested dockerd is this container's own,
# private, cold image cache — it cannot collide with containers/images already on the host
# (the dev box has stale `osiris-pg`/`osiris-redis` containers from prior work; a host-socket
# harness would either collide with those names or silently reuse their warm image layers,
# which is exactly the kind of quiet leak that makes an isolation instrument lie).
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

# git + ca-certificates/curl satisfy the (undocumented!) implicit prerequisite of having git
# at all — INSTALL.md's prerequisites list omits git even though step 1 is `git clone`, see
# the harness report. docker.io gives us dockerd+cli for the nested engine. nodejs/zstd are
# INSTALL.md's own "test-suite only" prerequisites (decision fb8e723c, thread 6d62ba2f) —
# test_js_syntax.py shells out to `node --check`, test_dsh_adapter.py to `zstd`; without
# them here step 7a's `uv run pytest` wall-hits on a genuinely clean stranger box.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl docker.io nodejs zstd \
    && rm -rf /var/lib/apt/lists/*

# uv, via the officially linked install method (docs/INSTALL.md links docs.astral.sh/uv,
# without saying how to install it — this is the one reasonable reading). uv then manages
# Python 3.12 itself off the lockfile's `requires-python` pin, so no system python3.12 is
# baked in here — a named substitution, not proof a system-python stranger sees the same
# thing. See the harness report for the full isolation boundary.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

COPY run.sh /usr/local/bin/stranger-run.sh
RUN chmod +x /usr/local/bin/stranger-run.sh

# The two proof-verification scripts (wave 6, dispatch 2c63770d) are harness plumbing, same
# class as run.sh itself — NOT part of the cloned `main` this harness replays (they assert
# against code already on `main`, they are not code under test), so they are baked into the
# image exactly the way run.sh already is, rather than expected to appear inside the clone.
COPY verify_manifest.py verify_read_alias.py /usr/local/bin/
RUN chmod +x /usr/local/bin/verify_manifest.py /usr/local/bin/verify_read_alias.py

ENTRYPOINT ["/usr/local/bin/stranger-run.sh"]
