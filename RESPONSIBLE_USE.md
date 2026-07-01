# Responsible use

Osiris has two faces (see [`README.md`](README.md)). Pointed at **your own work** (repos,
decisions, commits) it is project memory and raises none of the concerns below. Pointed at
the **public record** it produces claims about real people and organizations — and *that*
is powerful and dual-use: the same engine that exposes a fraud can be pointed at someone
for harassment. **This document governs the public-record face.** Read it before you use it
that way.

## No hands (both faces)

Osiris reads and *tells*; it does not act. It will not write to your repositories, commit,
send, or automate a mutation of any system. It produces a sourced finding; a human — or
Claude with a shell, or `git` — applies it. This is a deliberate boundary, not a missing
feature: an autonomous mutator you must supervise is the opposite of a tool you can trust,
so acting stays with hands that already have authority. See
[`ROADMAP.md`](ROADMAP.md#deliberately-not-done-and-why).

## Intended use (the public-record face)

Osiris is for **authorized investigation, journalism, due diligence, compliance, and
research** on the public record. It is *not* for stalking, harassment, doxxing, or
building a profile of a private individual to intimidate or endanger them.

If you publish or act on what Osiris surfaces, **you are responsible for verifying it.**
Osiris surfaces and *sources* the public record; it does not adjudicate truth.

## Two structural safeguards (and why they matter legally)

1. **Only public data.** Osiris federates open, published sources. It exposes nothing that
   was not already public. Every claim it emits is traceable to its source — which is the
   journalist's defense and yours. The provenance kernel exists so that a claim can survive
   a defamation challenge; that only works if you read claims *with* their provenance and
   confidence, and verify before publishing.
2. **Keyless by design = pointed at entities, not persons.** Because Osiris uses no paid
   data brokers and no private feeds, it structurally cannot reach private-person data. It
   federates the open *entity* commons — companies, filings, sanctioned parties, wallets —
   not your neighbour. This is a safety feature, not only a limitation. Do not bolt on
   data-broker or surveillance sources to defeat it.

## Accuracy is not optional

The deliverable has known data-quality noise (entity-resolution variants, geographic
inference edge cases — see [`ROADMAP.md`](ROADMAP.md#known-noise)). Provenance makes errors
**auditable, not absent.** Read the evidence tiers and the sources. A claim graded
*speculative* or *co-occurrence* is a lead to verify, not a fact to publish. A merge you
didn't confirm is a hypothesis. Treating Osiris output as ground truth is misuse.

## Data-source licenses (read before commercial use)

Osiris is AGPL-3.0, but the **data each source provides has its own license**, and those
travel with the data, not with this code. The notable ones:

| Source | License / terms | Note |
|--------|-----------------|------|
| **OpenSanctions** | **CC-BY-NC 4.0 (non-commercial)** | ⚠️ Commercial use requires a paid license from OpenSanctions. This is the sharpest edge — if you use the sanctions/PEP base commercially, get a license. |
| SEC EDGAR | U.S. public domain | Fair-access User-Agent expected. |
| GLEIF (LEI) | CC0 | Open. |
| Wikidata | CC0 | Open. |
| CourtListener (Free Law Project) | Permissive; API terms apply | Respect rate limits. |
| ClinicalTrials.gov | U.S. public domain | — |
| OrgBook BC | Open Government Licence – BC | — |
| Etherscan | API Terms of Service | User-supplied key; you accept their ToS. |

Verify the current terms of any source yourself before relying on them — licenses change,
and this table is guidance, not legal advice. **Continuous/automated collection** (the
monitoring capability) raises additional rate-limit and ToS obligations: respect the
per-origin rate limits Osiris enforces, and don't disable them.

## No warranty

Osiris is provided as-is, without warranty of any kind. The authors are not liable for how
it is used or for decisions made from its output. See [`LICENSE`](LICENSE).
