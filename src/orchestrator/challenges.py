"""Centralized challenge detection (DESIGN §7).

Helpers never duplicate this — the router/runner calls detect() on a server-side
response and, on a hit, suspends the run to a human handoff. We never try to
solve or evade; detection only routes the work to the analyst's real browser.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass


class ChallengeKind(enum.Enum):
    CLOUDFLARE = "cloudflare"
    TURNSTILE = "turnstile"
    HCAPTCHA = "hcaptcha"
    RECAPTCHA = "recaptcha"
    AKAMAI = "akamai"
    PERIMETERX = "perimeterx"
    LOGIN_WALL = "login_wall"
    HTML_COLLAPSE = "html_collapse"


@dataclass(frozen=True)
class Challenge:
    kind: ChallengeKind
    detail: str


class ChallengeDetected(Exception):
    """Raised by a connector when a server-side fetch is blocked; carries the
    handoff context so dispatch can suspend the run."""

    def __init__(self, challenge: Challenge, *, url: str) -> None:
        super().__init__(f"{challenge.kind.value}: {challenge.detail}")
        self.challenge = challenge
        self.url = url


def detect(
    *,
    status_code: int,
    headers: dict[str, str],
    body: str,
    baseline_len: int | None = None,
) -> Challenge | None:
    """Return the first matching challenge signature, or None if the response
    looks like genuine content."""
    h = {k.lower(): v.lower() for k, v in headers.items()}
    b = body.lower()
    cookies = h.get("set-cookie", "")

    if "/cdn-cgi/challenge" in b or "just a moment" in b or "cf-mitigated" in h:
        return Challenge(ChallengeKind.CLOUDFLARE, "Cloudflare interstitial")
    if "challenges.cloudflare.com/turnstile" in b or "cf-turnstile" in b:
        return Challenge(ChallengeKind.TURNSTILE, "Turnstile widget")
    if "hcaptcha.com" in b or "h-captcha" in b:
        return Challenge(ChallengeKind.HCAPTCHA, "hCaptcha")
    if "recaptcha" in b or "g-recaptcha" in b:
        return Challenge(ChallengeKind.RECAPTCHA, "reCAPTCHA")
    if "_abck" in cookies or "akamai" in h.get("server", ""):
        return Challenge(ChallengeKind.AKAMAI, "Akamai sensor")
    if "_px" in cookies:
        return Challenge(ChallengeKind.PERIMETERX, "PerimeterX cookie")
    if status_code == 401 or "sign in to continue" in b or "/login" in h.get("location", ""):
        return Challenge(ChallengeKind.LOGIN_WALL, "Login wall")
    # sudden HTML collapse vs. a known baseline (bot-fight stub pages)
    if baseline_len is not None and baseline_len > 2000 and len(body) < baseline_len * 0.2:
        return Challenge(ChallengeKind.HTML_COLLAPSE, f"{len(body)} bytes vs {baseline_len}")
    return None
