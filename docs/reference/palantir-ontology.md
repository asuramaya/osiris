<!-- source: https://www.palantir.com/docs/foundry/ontology/core-concepts | vendor: palantir | topic: ontology core concepts | grounds: src/ontology/schema.py -->
# Palantir Foundry — Ontology core concepts

The semantic-layer model Osiris's `src/ontology/schema.py` descends from: a typed catalog over
the store, with a separate kinetic layer for changes. Osiris adds two kernel-wide enrichments
Palantir doesn't have — **evidence-grading** and **event-sourcing** — but the shape is this.

## What the Ontology is
A rich **semantic layer** on top of integrated datasets/models — a digital twin that maps raw
data to structured, queryable types.

## Core building blocks
- **Object Type** — the schema of a real-world entity/event; an *object* is one instance, an
  *object set* a collection. (Osiris: `ObjectType` in the schema catalog.)
- **Property** — the schema of a characteristic of an object type; a *property value* is the
  data on a specific object. (Osiris: assertions, multi-source + evidence-graded.)
- **Link Type** — the schema of a relationship between two object types; a *link* is one
  instance. (Osiris: `LinkType`, with domain/range.)
- **Action Type** — the schema of a set of edits to objects/properties/links a user can take
  at once, with side effects. (Osiris: the six Actions — the only write path.)
- **Function** — code taking inputs → outputs, natively integrated to read objects/sets.

## Semantic vs kinetic
The **semantic layer** = types (object/link/property) defining structure. The **kinetic layer**
= action types + functions that modify and interact. Osiris keeps this split exactly: the
schema catalog is read-only truth; `Actions` is the sole kinetic waist (audited, event-sourced).
