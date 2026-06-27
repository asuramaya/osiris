"""Person vs Organization — decide what KIND of entity a bare name is.

Several ingests mint a node typed Person or Organization from a name string. When the
source doesn't disambiguate, a blind default poisons the graph: SEC Form D lists
"related persons" that are frequently the GP ENTITY ("Brilliant Phoenix GP Inc.",
"LLC Sydecar", "Finally Fund Admin LLC"), and typing those as Person creates fake people
that never cross-base-resolve and pollute every principals/screening read.

This decides from the name itself, CONSERVATIVELY: an ambiguous bare name stays a Person.
We only call something an Organization on a strong signal (a legal-form token, a curated
org-noun, an ampersand, or a digit) — so we never turn a real person into a company.
"""

from __future__ import annotations

import re

# Legal-form tokens — their presence ⇒ an organization.
_LEGAL = frozenset({
    "inc", "incorporated", "corp", "corporation", "co", "company", "llc", "llp", "lp",
    "gp", "ltd", "limited", "plc", "sa", "sas", "sl", "ag", "gmbh", "kg", "srl", "spa",
    "nv", "bv", "ab", "oy", "as", "kk", "pty", "pte", "se", "oao", "ooo", "pao", "zao",
    "pjsc", "ojsc", "jsc", "ao", "kft", "doo", "dd", "tov", "sro", "mic", "spv", "ulc",
})
# Organization-noun tokens — curated to avoid common surnames (no "bank"/"white"/"king").
_ORG_WORDS = frozenset({
    "fund", "funds", "capital", "partners", "ventures", "venture", "management",
    "holdings", "holding", "advisors", "advisers", "associates", "group", "trust",
    "partnership", "syndicate", "technologies", "solutions", "systems", "labs",
    "laboratories", "industries", "enterprises", "investments", "equity", "securities",
    "financial", "bancorp", "realty", "properties", "foundation", "institute",
    "association", "committee", "series", "spac", "acquisition", "mortgage", "fintech",
    "vc",
})
# Generational/credential suffixes that DON'T make a name an organization.
_PERSON_SUFFIX = frozenset({"jr", "sr", "ii", "iii", "iv", "phd", "md", "esq", "cpa", "cfa"})

_NA = re.compile(r"\b(n/?a|none|not applicable)\b", re.I)
_TOK = re.compile(r"[a-z0-9&]+")


def clean_entity_name(name: str) -> str:
    """Strip Form D 'N/A' placeholder tokens (firstName='N/A' lastName='X Inc.' ->
    'N/A X Inc.') and collapse whitespace/edge punctuation."""
    # strip spaces/commas at the edges but KEEP a trailing '.' ("Inc." is an abbreviation)
    return " ".join(_NA.sub(" ", name).split()).strip(" ,")


def is_organization(name: str) -> bool:
    """True iff the name carries a strong organization signal. Conservative — a plain
    personal name ('Chunhua Wu', 'S. Leon') returns False."""
    low = clean_entity_name(name).lower()
    if not low:
        return False
    toks = _TOK.findall(low)
    core = [t for t in toks if t not in _PERSON_SUFFIX]
    if not core:
        return False
    if any(t in _LEGAL or t in _ORG_WORDS for t in core):
        return True
    if "&" in low:                       # "Smith & Co", "Wilson & Wesson" -> org
        return True
    # NB: a bare digit is NOT a signal — real orgs already carry a word token
    # ("Fund 2" has 'fund'), while people legitimately have digits in the source
    # ('Desiree Lambert Inmate No. 13432-046'). The digit rule was a false-positive magnet.
    return False


def classify_entity_type(name: str) -> str:
    """'Organization' or 'Person' for a bare name (the ontology types)."""
    return "Organization" if is_organization(name) else "Person"


# Tokens that betray a CONTACT STRING masquerading as a person name (ClinicalTrials
# 'overallOfficials' sometimes carries "Call 1-877-CTLILLY ..." or "Clinical
# Transparency (dept. 2834)" instead of an investigator).
_NOT_A_NAME = re.compile(r"\d|@|https?://|www\.|\bcall\b|\bdept\b|\bdepartment\b|\bhotline\b", re.I)


def is_plausible_person_name(name: str) -> bool:
    """A conservative gate before minting a Person: reject org-shaped names and obvious
    contact strings (phone/email/url/'call'/'dept'/digits) and sentence-length blobs."""
    s = clean_entity_name(name)
    if not s or is_organization(s) or _NOT_A_NAME.search(s):
        return False
    toks = s.split()
    return 2 <= len(toks) <= 5   # a real name is 2–5 tokens, not one word or a sentence
