from __future__ import annotations

from src.orchestrator.challenges import ChallengeKind, detect


def test_detects_cloudflare_interstitial() -> None:
    c = detect(status_code=403, headers={}, body="<title>Just a moment...</title>")
    assert c is not None and c.kind is ChallengeKind.CLOUDFLARE


def test_detects_turnstile_and_captchas() -> None:
    assert detect(status_code=200, headers={}, body='<div class="cf-turnstile">').kind \
        is ChallengeKind.TURNSTILE
    assert detect(status_code=200, headers={}, body="<iframe src=hcaptcha.com>").kind \
        is ChallengeKind.HCAPTCHA
    assert detect(status_code=200, headers={}, body='<div class="g-recaptcha">').kind \
        is ChallengeKind.RECAPTCHA


def test_detects_login_wall_and_vendor_cookies() -> None:
    assert detect(status_code=401, headers={}, body="").kind is ChallengeKind.LOGIN_WALL
    assert detect(
        status_code=200, headers={"Set-Cookie": "_abck=xyz"}, body=""
    ).kind is ChallengeKind.AKAMAI
    assert detect(
        status_code=200, headers={"Set-Cookie": "_px3=abc"}, body=""
    ).kind is ChallengeKind.PERIMETERX


def test_html_collapse_against_baseline() -> None:
    assert detect(status_code=200, headers={}, body="x" * 100, baseline_len=10000).kind \
        is ChallengeKind.HTML_COLLAPSE


def test_clean_content_is_not_a_challenge() -> None:
    body = "<html><body>" + "real content " * 500 + "</body></html>"
    assert detect(status_code=200, headers={"Server": "nginx"}, body=body, baseline_len=6000) \
        is None
