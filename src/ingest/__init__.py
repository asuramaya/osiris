"""Bulk ingest of pre-built open data bases (the 'stand on a finished base' move).

Unlike the cascade (per-object, triggered crawl), these loaders take an already-
normalized open dataset — OpenSanctions, GLEIF, OpenAlex, Wikidata dumps — and pour
it into the graph through the Actions layer in one pass. No crawl treadmill: the
nonprofit/government that published the dump already paid the base cost; we federate.
"""
