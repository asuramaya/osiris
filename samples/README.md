# Samples

Real outputs from Osiris, generated from **public records** as demonstrations. Nothing
here is hand-edited — these are the literal `dossier_report` and evidence exports the
engine produces.

| File | What it is |
|------|-----------|
| [`celsius-network.dossier.md`](celsius-network.dossier.md) | Markdown dossier for Celsius Network — a documented crypto collapse where the financing *is* the story (Form D officers resolve to the criminally-charged principals; litigation is the bankruptcy + clawback suits). |
| [`neuralink.dossier.md`](neuralink.dossier.md) | Markdown dossier for Neuralink Corp. — the Form D feeder-SPV financing swarm, co-investment operators, and the operational-vs-disclosed geography discrepancy (Canada / Indonesia / UAE / UK from trial sites and feeder funds). |
| [`*.evidence.json`](celsius-network.evidence.json) | The graded provenance behind one entity — each fact with its `source`, `evidence_class`, `confidence`, and `observed` date. This is the kernel made visible. |

## Read these honestly

These are public companies/figures and the claims are sourced — but the samples are shown
**with their warts**, because that is the point of a provenance-first engine:

- **Multi-source values coexist.** Celsius's `incorporation_state` shows both `NJ` and the
  raw EDGAR code `X0` (= "unknown" in a filing) — Osiris surfaces what was filed, it does
  not silently pick one.
- **Resolution is imperfect.** Some neighbor names carry parser artifacts (e.g. an `n/a`
  prefix) pending a review pass.
- **Every claim is traceable.** That is what makes an error *auditable* rather than
  *hidden* — read the `evidence_class` and `source` before you believe a line.

A claim graded `co_occurrence` or `derived`, or a merge you didn't confirm, is a **lead to
verify, not a fact to publish.** See [`../RESPONSIBLE_USE.md`](../RESPONSIBLE_USE.md).

## Regenerate

These were produced by calling `dossier_report` (and a small evidence export) against a
populated graph — the same path an MCP client or the CLI takes:

```python
from src.dissemination.dossier_report import build_dossier_report
md = await build_dossier_report(pool, object_id)
```
