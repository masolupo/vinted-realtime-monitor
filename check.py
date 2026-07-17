"""
check.py — audit abandoned ids. Probe a list of item ids and report, at a glance, how many
are real listings (a 307 we missed) vs genuine holes (a permanent 404 — a draft/deleted slot
that was never a listing). Feed it the gave-up log the monitor writes with GAVEUP_LOG.

Usage:
    python check.py 9370454330 9370454333            # specific ids
    python check.py gaveup.log                        # a file of ids (one per line)
    GAVEUP_LOG=gaveup.log python monitor.py           # …first record them, then check the log
"""

import asyncio
import os
import sys

from extractor import SessionFactory
from vinted import BASE, PROBE_TIMEOUT, err, require_proxy

CONCURRENCY = 20


async def check_id(factory, sem, item_id):
    async with sem:
        ext = factory.acquire()
        if ext is None:
            return item_id, None, ""
        code, loc = None, ""
        try:
            r = await ext.session.get(f"{BASE}/items/{item_id}", timeout=PROBE_TIMEOUT, allow_redirects=False)
            code = r.status_code
            loc = r.headers.get("location") or r.headers.get("Location") or ""
        except Exception:
            code = "net"
        finally:
            await factory.release(ext, "absent", "")  # keep the session (this isn't a ban)
        return item_id, code, loc


async def main():
    require_proxy()

    ids = []
    for arg in sys.argv[1:]:
        if arg.isdigit():
            ids.append(int(arg))
        elif os.path.isfile(arg):
            with open(arg) as f:
                ids.extend(int(x) for x in f.read().split() if x.strip().isdigit())
    ids = sorted(set(ids))
    if not ids:
        err("usage: python check.py <id> [<id> …] | <file-of-ids>")
        return

    err(f"checking {len(ids)} ids (this re-probes them now, minutes after they were seen)…")
    factory = SessionFactory(target=CONCURRENCY, create_concurrency=CONCURRENCY)
    await factory.prewarm(CONCURRENCY)
    if factory.live_count() == 0:
        err("could not create sessions — check the proxy.")
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    try:
        results = await asyncio.gather(*(check_id(factory, sem, i) for i in ids))
    finally:
        await factory.close()

    misses = holes = other = 0
    for item_id, code, loc in results:
        if code in (301, 302, 307, 308):
            misses += 1
            slug = loc.rstrip("/").split("/")[-1]
            print(f"  MISS  #{item_id}  307 -> {slug}")
        elif code == 404:
            holes += 1
        else:
            other += 1
            print(f"  ?     #{item_id}  {code}")

    print("\n=== SUMMARY (audit of ABANDONED ids only — NOT a coverage figure) ===")
    print(f"checked          : {len(ids)}")
    print(f"404  (real holes): {holes}   — never a listing (draft/deleted), correctly skipped")
    print(f"307  (MISSES)    : {misses}   — real listings that appeared after MAX_AGE")
    print(f"other            : {other}   — anti-bot 200 pages masking a 404, or a blip")
    print(
        "\nThese are only the ids the monitor GAVE UP on — a small slice of everything it read,\n"
        "NOT a percentage of all listings. A 307 here is a real listing that surfaced after\n"
        "MAX_AGE; raise MAX_AGE to catch more. Overall coverage ≈ 1 − (307s ÷ total listings\n"
        "read), which the monitor's heartbeat reports as `read` vs `gave-up` — e.g. 10 such\n"
        "307s against ~7000 read is ~99.9% coverage, not '10% missed'."
    )


if __name__ == "__main__":
    asyncio.run(main())
