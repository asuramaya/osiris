"""ThreatFox (abuse.ch) IOC parser.

Consumes a Malware object; for each IOC record ThreatFox returns, emits:
  * an Indicator object (the IOC), with its ThreatFox properties
  * an ObservedData object (the raw record, content-addressed as evidence)
  * Indicator --indicates--> the input Malware   (IOC attributes the malware)
  * Indicator --based-on--> ObservedData         (STIX provenance edge)

The IOC thereby plugs into the existing ATT&CK graph through the shared Malware
node: Malware <--uses-- IntrusionSet and Malware --... so a fresh OSINT IOC
becomes reachable from the actor's TTP/TTAL picture. (Direct IOC->technique
links are added when a record carries an ATT&CK tag; ThreatFox usually doesn't.)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.parsers.base import InputObject, LinkSpec, ObjectSpec, ParseResult, TargetRef

_TF_TECHNIQUE_PREFIX = "attack."  # e.g. tag "attack.T1566" -> ATT&CK T1566


def _parse_ts(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=UTC)
    except ValueError:
        # ThreatFox uses "YYYY-MM-DD HH:MM:SS UTC"
        return datetime.strptime(raw.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def parse_threatfox_iocs(response: dict[str, Any], input_object: InputObject) -> ParseResult:
    result = ParseResult()
    if response.get("query_status") != "ok":
        return result

    latest: datetime | None = None
    for rec in response.get("data", []):
        ioc_value = rec.get("ioc")
        ioc_type = rec.get("ioc_type", "unknown")
        tf_id = rec.get("id")
        if not ioc_value or tf_id is None:
            continue

        first_seen = _parse_ts(rec.get("first_seen"))
        latest = first_seen if latest is None else max(latest, first_seen)
        confidence = float(rec.get("confidence_level", 50)) / 100.0

        ind_canonical = f"ioc:{ioc_type}:{ioc_value}"
        obs_canonical = f"threatfox:{tf_id}"

        result.objects.append(
            ObjectSpec(
                type="Indicator",
                canonical=ind_canonical,
                confidence=confidence,
                properties={
                    "pattern_value": ioc_value,
                    "ioc_type": ioc_type,
                    "threat_type": rec.get("threat_type"),
                    "malware_printable": rec.get("malware_printable"),
                    "tags": rec.get("tags") or [],
                    "reference": rec.get("reference"),
                },
            )
        )
        result.objects.append(
            ObjectSpec(
                type="ObservedData",
                canonical=obs_canonical,
                confidence=0.99,
                properties={"source": "threatfox", "threatfox_id": tf_id},
                evidence=rec,  # runner content-addresses this and stamps the hash
            )
        )
        # IOC attributes the consumed malware
        result.links.append(
            LinkSpec(
                from_ref=TargetRef(ref=ind_canonical),
                to_ref=TargetRef(input=True),
                type="indicates",
                confidence=confidence,
            )
        )
        # Indicator based-on its raw observation
        result.links.append(
            LinkSpec(
                from_ref=TargetRef(ref=ind_canonical),
                to_ref=TargetRef(ref=obs_canonical),
                type="based-on",
                confidence=0.99,
            )
        )
        # any explicit ATT&CK technique tag -> direct indicates->AttackPattern
        for tag in rec.get("tags") or []:
            if isinstance(tag, str) and tag.lower().startswith(_TF_TECHNIQUE_PREFIX):
                tid = tag.split(".", 1)[1].upper()
                result.links.append(
                    LinkSpec(
                        from_ref=TargetRef(ref=ind_canonical),
                        to_ref=TargetRef(external_id=tid),
                        type="indicates",
                        confidence=confidence,
                    )
                )

    result.observed_at = latest or datetime.now(UTC)
    return result
