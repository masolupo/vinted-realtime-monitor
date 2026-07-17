"""
vinted.py — the Vinted-facing layer: config, a warmed session, and the ID-frontier probes.

Everything here is "how to talk to Vinted": build a TLS-impersonating session through the
proxy, probe a single item id (307 = born, 404 = not yet), and locate the live id frontier
from the (slightly stale) search catalog. The layers above — the session pool in
extractor.py and the chase loop in monitor.py — build on these primitives.
"""

import asyncio
import os
import sys
from random import randint

from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

# Load a local .env if present (optional). Nothing here is secret except the proxy
# string — see .env.example. Real env vars still take precedence.
load_dotenv()

DOMAIN = os.getenv("VINTED_DOMAIN", "www.vinted.it")
PROXY = os.getenv("HTTP_PROXY") or os.getenv("PROXY")
BASE = f"https://{DOMAIN}"
CATALOG = f"{BASE}/api/v2/catalog/items?order=newest_first&per_page=96"

# Vinted is geo-partitioned: the Accept-Language header must match the domain's country,
# or the endpoints answer in the wrong locale — or not at all. We derive it from the TLD
# so you only have to set VINTED_DOMAIN correctly (see .env.example).
LANG_BY_TLD = {
    "it": "it-IT,it;q=0.9",
    "fr": "fr-FR,fr;q=0.9",
    "de": "de-DE,de;q=0.9",
    "es": "es-ES,es;q=0.9",
    "nl": "nl-NL,nl;q=0.9",
    "be": "nl-BE,nl;q=0.9,fr;q=0.8",
    "pl": "pl-PL,pl;q=0.9",
    "pt": "pt-PT,pt;q=0.9",
    "cz": "cs-CZ,cs;q=0.9",
    "sk": "sk-SK,sk;q=0.9",
    "lt": "lt-LT,lt;q=0.9",
    "at": "de-AT,de;q=0.9",
    "lu": "fr-LU,fr;q=0.9",
    "ro": "ro-RO,ro;q=0.9",
    "hu": "hu-HU,hu;q=0.9",
    "se": "sv-SE,sv;q=0.9",
    "fi": "fi-FI,fi;q=0.9",
    "dk": "da-DK,da;q=0.9",
    "gr": "el-GR,el;q=0.9",
    "hr": "hr-HR,hr;q=0.9",
    "uk": "en-GB,en;q=0.9",  # www.vinted.co.uk
}


def accept_language(domain: str) -> str:
    """The Accept-Language matching the Vinted domain's country (fallback: English)."""
    tld = domain.rstrip("/").split(".")[-1].lower()
    return LANG_BY_TLD.get(tld, "en-US,en;q=0.9")


# Default headers for every session — the locale follows the domain, not a fixed value.
HEADERS = {"Accept-Language": accept_language(DOMAIN)}

# Frontier search / probe robustness. A probe is retried through transient failures so a
# 429 / network blip is never mistaken for "not there"; the frontier search samples a small
# cluster of ids so a lone hole below the frontier is not mistaken for "above it".
PROBE_TIMEOUT = float(os.getenv("PROBE_TIMEOUT", "2"))    # per-probe timeout, seconds (~3-6× the typical ~0.4s)
PROBE_RETRIES = 4        # extra attempts on 429/403/network before giving up
PROBE_BACKOFF = 0.4      # base backoff between attempts, seconds (doubles each time)
FRONTIER_CLUSTER = 16    # ids sampled per frontier probe; must exceed the longest hole-run
CATALOG_SAMPLES = int(os.getenv("CATALOG_SAMPLES", "5"))  # catalog reads to take the freshest of


def err(msg):
    """Status/diagnostics go to stderr so the id+title stream on stdout stays clean."""
    print(msg, file=sys.stderr, flush=True)


def require_proxy():
    """A proxy is mandatory: hitting Vinted from your own IP gets it blocked fast.
    Stop with a clear message instead of burning the user's real address."""
    if not PROXY:
        raise SystemExit(
            "No proxy configured. A proxy is REQUIRED — running this against your "
            "own IP will get it blocked by Vinted. Set HTTP_PROXY in your .env "
            "(copy .env.example), using an IP in the same country as VINTED_DOMAIN."
        )


async def make_session():
    """A TLS-impersonating session with warmed-up cookies. None if the proxy is dead."""
    for _ in range(5):
        session = AsyncSession(impersonate="chrome", proxy=PROXY, headers=HEADERS)
        try:
            r = await session.get(BASE, timeout=15)
            if r.status_code == 200:
                return session
            await session.close()
        except Exception:
            try:
                await session.close()
            except Exception:
                pass
    return None


def title_from_location(location: str) -> str:
    """`/items/{id}-{slug}` -> the slug, turned back into a readable title."""
    if not location:
        return ""
    slug = location.rstrip("/").split("/")[-1]
    parts = slug.split("-", 1)
    return parts[1].replace("-", " ") if len(parts) > 1 else ""


async def probe(session, item_id):
    """Return ('born', title) | ('absent', '') | ('error', reason).

    On the error path the second field carries a reason (the HTTP status code, or 'net'
    for a network/timeout exception) so callers can see *why* a probe failed — e.g. a
    burst of 403/429 means Vinted's anti-bot is throttling us, 'net' means the proxy is
    choking. Callers that only care about born/absent ignore it."""
    try:
        r = await session.get(f"{BASE}/items/{item_id}", timeout=PROBE_TIMEOUT, allow_redirects=False)
        if r.status_code in (301, 302, 307, 308):
            location = r.headers.get("location") or r.headers.get("Location") or ""
            return "born", title_from_location(location)
        if r.status_code == 404:
            return "absent", ""
        return "error", str(r.status_code)
    except Exception:
        return "error", "net"


async def probe_settled(session, item_id, retries=PROBE_RETRIES):
    """Like probe(), but retries through transient failures (429 / 403 / network) with
    exponential backoff so the answer is definitive. A 404 is returned immediately — it
    is a real "absent", not a failure. Returns ('error', '') only if every attempt failed."""
    delay = PROBE_BACKOFF
    for attempt in range(retries + 1):
        status, title = await probe(session, item_id)
        if status in ("born", "absent"):
            return status, title
        if attempt < retries:
            await asyncio.sleep(delay)
            delay *= 2
    return "error", ""


async def region_is_live(session, item_id, span=FRONTIER_CLUSTER):
    """True if any id in [item_id, item_id + span) is born — i.e. item_id sits at or below
    the live frontier. Probes the whole cluster CONCURRENTLY (not one-by-one), so an
    'above the frontier' region — where every id is 404 — costs a single round-trip instead
    of `span` of them. The cluster (rather than a single id) keeps a lone hole below the
    frontier from being mistaken for 'above it'."""
    results = await asyncio.gather(
        *(probe_settled(session, item_id + i) for i in range(span))
    )
    return any(status == "born" for status, _ in results)


async def _read_catalog_newest(session):
    """One catalog read → the newest id it contains, or None on any failure. A random
    cache-buster query param makes each read miss any stale CDN-cached snapshot, so the
    frontier search is never seeded from an hours-old catalog page (which would send it
    climbing millions of ids to recover)."""
    try:
        r = await session.get(f"{CATALOG}&_={randint(0, 2_000_000_000)}", timeout=8)
        items = r.json().get("items")
        if items:
            return int(items[0]["id"])
    except Exception:
        pass
    return None


async def newest_catalog_id(session, samples=CATALOG_SAMPLES, retries=PROBE_RETRIES):
    """Highest id currently in the search index — the seed for the frontier search.

    A rotating proxy will occasionally route a catalog read to an exit that serves a STALE,
    cached snapshot (an id millions behind the live frontier). Seeding the frontier search
    from that would send it climbing a huge phantom gap and take forever. To be immune we
    read the catalog `samples` times concurrently — each call rotates to a different exit
    through the pool — and take the HIGHEST id: the freshest exit wins, stale ones are
    harmless. Retries the whole round if every read failed."""
    delay = PROBE_BACKOFF
    for attempt in range(retries + 1):
        reads = await asyncio.gather(*(_read_catalog_newest(session) for _ in range(samples)))
        ids = [r for r in reads if r is not None]
        if ids:
            return max(ids)
        if attempt < retries:
            await asyncio.sleep(delay)
            delay *= 2
    raise RuntimeError(
        "Could not read the catalog after retries — the proxy is flaky or blocked. "
        "Check that its country matches VINTED_DOMAIN, then try again."
    )


async def find_frontier(session, start_id):
    """Highest id that currently exists, located from a catalog id that may be anywhere from a
    few hundred to *millions* of ids stale (Vinted sometimes serves an old catalog snapshot).
    Fully concurrent so startup is a handful of round-trips regardless of how far off the seed
    is: one wide geometric scan brackets the frontier in a single round (no slow sequential
    climb), then dense concurrent grids refine the bracket. Every point is tested with
    region_is_live() (a concurrent cluster), so a hole never fools it."""
    err(f"  locating the live frontier above catalog id {start_id}…")

    # Phase 1 — one wide geometric scan. The offsets span up to ~130M, so even a badly stale
    # seed is bracketed in a single round-trip; keep the highest live point and lowest dead one.
    offsets = (0, 2_000, 16_000, 128_000, 1_000_000, 8_000_000, 32_000_000, 130_000_000)
    points = [start_id + o for o in offsets]
    flags = await asyncio.gather(*(region_is_live(session, p) for p in points))
    low, high = start_id, None
    for p, live in zip(points, flags):
        if live:
            low = p
        elif high is None:
            high = p
    if high is None:
        # Even +130M is live (absurd) — fall back to a short exponential walk.
        low, step = points[-1], 130_000_000
        high = low + step
        while await region_is_live(session, high):
            low, step = high, step * 2
            high = high + step

    err(f"  frontier bracketed in [{low}, {high}] — refining…")

    # Phase 2 — dense concurrent grid: each round probes GRID_POINTS points at once and keeps
    # the sub-interval where live flips to dead, shrinking the bracket ~GRID_POINTS× per round.
    GRID_POINTS = 12
    while high - low > 1:
        step = max(1, (high - low) // (GRID_POINTS + 1))
        mids = list(range(low + step, high, step))
        if not mids:
            break
        flags = await asyncio.gather(*(region_is_live(session, m) for m in mids))
        for m, live in zip(mids, flags):
            if live:
                low = m
            else:
                high = m
                break
    return low
