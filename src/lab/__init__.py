"""Frontier-policy experiment harness (offline, kernel-read-only).

The cascade is network-bound and non-deterministic, so policies can't be tested
against the live world. This package replays a *recorded or synthetic* substrate
thousands of times under pluggable frontier policies, scores each against ground
truth, and asks the only question that matters: does any biological coupling beat
the naked baseline (personalized PageRank / Thompson bandit)? Nothing here imports
or mutates the cascade; it borrows only the evidence-class vocabulary so the lab
speaks the same language as prod.
"""
