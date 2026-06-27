# Security

## Reporting a vulnerability

If you find a security issue — especially one that could leak data, expose secrets, or let
a connector be abused — please **open a private report** (GitHub Security Advisory) or
email the maintainer rather than filing a public issue. Give it a few days before public
disclosure.

## Deployment posture (v0.1)

Osiris v0.1 is designed for a **single operator on a single machine**:

- All services bind to `127.0.0.1`. There is **no authentication layer** and **no
  multi-tenant isolation** — do not expose the API or MCP server to a network you don't
  fully trust. The single static identity (`OSIRIS_ACTOR`) is an audit attribution, not an
  access control.
- The kernel (Postgres) is the source of truth — back it up; everything else is
  restartable.
- Multi-user / hosted deployment (auth, a real secret manager, network isolation) is on the
  [roadmap](ROADMAP.md#deployment-as-a-sequence-of-cuts-not-a-rewrite), not in v0.1. Don't
  run it as a shared service yet.

## Secrets & trust zones

- API keys and the lease-encryption key live in `.env` / OS keyring, never in the repo.
  `.env` is gitignored; verify nothing secret is committed before you push a fork.
- The cookie-lease subsystem encrypts captured browser sessions at rest (Fernet); the
  decryption key is held closest to the operator. The kernel can store an encrypted lease
  it cannot itself read — the key stays at the edge. (Experimental in v0.1.)
- Connectors make outbound requests to third-party services under your identity / keys and
  must respect those services' Terms of Service and rate limits. See
  [`RESPONSIBLE_USE.md`](RESPONSIBLE_USE.md).
