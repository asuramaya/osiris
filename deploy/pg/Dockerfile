# osiris-pg: stock postgres:16 plus pg_repack, so nightly compaction can rewrite a bloated
# table without holding an ACCESS EXCLUSIVE lock for its whole length (operator decision 2,
# 2026-11-12; measured live: reads blocked 13 ms, writes 68 ms, against about 350 ms for
# VACUUM FULL on the same table, and 10-25 s for VACUUM FULL at live table sizes).
#
# The base is the exact postgres:16 image the live box ran (16.14). The PGDG package
# postgresql-16-repack 1.5.3 depends on postgresql-16 > 16.16 and Docker Hub carries no
# 16.16 tag, so apt moves the server to 16.16 here: a minor release, same on-disk format,
# no dump or restore. The extension itself is created once with
# `CREATE EXTENSION pg_repack` (src/orchestrator/db_compaction.py stays on VACUUM FULL
# until it finds both the extension and the pg_repack binary in this container).
FROM postgres:16@sha256:4b7183ac05f8ef417db21fd72d71047a4238340c261d3cc3ddb6d579ab5071ae
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-16-repack \
 && rm -rf /var/lib/apt/lists/*
ENV PG_VERSION=16.16-1.pgdg13+1
