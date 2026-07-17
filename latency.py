"""
latency.py — Prove the lead: how many seconds the ID-frontier approach beats the catalog.

It places "sentinels" on ids just ABOVE the live frontier (so they're still 404 — not born
yet), then watches each one two ways at once:

  * by ID      — GET /items/{id}, flips 404 -> 307 at the birth instant
  * by catalog — the catalog/items?newest_first search index, the ~8s-stale way everyone uses

Each sentinel is polled the way a real bot polls the frontier: PIPELINED. A new probe is fired
every ID_SPACING seconds WITHOUT waiting for the previous one to come back, so a few are in
flight at once and the effective poll rate (calls/s per sentinel) is 1/ID_SPACING — independent
of the proxy round-trip. (Strictly sequential polling caps at 1/RTT ≈ 1.4/s on a slow residential
proxy; pipelining is how you get above that, and it's what any serious frontier bot actually does.)

To stay fast and FRESH it does NOT use the iterative find_frontier (which can take a minute and
go stale under heavy traffic). Instead it localises the frontier with a single concurrent sweep
(~2 round-trips), then drops the sentinels a safe margin above it. It only counts a sentinel once
it has *witnessed* the 404 -> 307 flip, so any placed too low (already born) are skipped, not timed.

The lead ≈ (catalog-behind-frontier gap) ÷ (mint rate), so it's biggest — and births slowest —
at low-traffic hours. Run it during busy hours for the representative ~6-8s.

Educational / research use. A proxy is REQUIRED (see .env.example). Read the DISCLAIMER.

Usage:
    python latency.py
    MATCHES=15 python latency.py
"""

import asyncio
import os
import statistics
import time

from dotenv import load_dotenv

from extractor import RotatingSession, SessionFactory
from vinted import CATALOG, err, newest_catalog_id, probe, require_proxy

load_dotenv()

DURATION = int(os.getenv("DURATION", "150"))       # hard cap on the watch, seconds (stops early — see MATCHES)
MATCHES = int(os.getenv("MATCHES", "6"))           # stop as soon as this many leads are measured
SENTINELS = int(os.getenv("SENTINELS", "16"))      # how many ids to track
MARGIN = int(os.getenv("MARGIN", "3500"))          # place sentinels this far above the swept frontier (drift headroom)
SENT_STEP = int(os.getenv("SENT_STEP", "150"))     # spacing between sentinels (spreads their births a little)
CATALOG_POLL = float(os.getenv("CATALOG_POLL", "0.3"))
ID_SPACING = float(os.getenv("ID_SPACING", "0.4")) # fire a probe per sentinel this often → ~2.5 calls/s, pipelined
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "3")) # cap concurrent probes per sentinel (keeps >=2/s up to ~1.5s RTT)
STALL = int(os.getenv("STALL", "12"))              # once births are done, stop this long after the last catalog match
# The steady concurrent demand of pipelined probing is only ~SENTINELS * RTT/ID_SPACING sessions,
# well under POOL, so we pre-warm just PREWARM of them (fast) and let the background replenisher
# fill the pool up to POOL during the ~20s wait for the first births — no probe ever starves.
PREWARM = int(os.getenv("PREWARM", "50"))          # sessions ready before we start (must cover the frontier sweep)
POOL = int(os.getenv("POOL", "56"))                # pool the replenisher tops up to (spike headroom over steady demand)


def stamp(t):
    """Wall-clock time with milliseconds, e.g. 18:48:10.242."""
    return time.strftime("%H:%M:%S", time.localtime(t)) + f".{int((t % 1) * 1000):03d}"


async def quick_probe(factory, item_id):
    """One-off probe used by the frontier sweep; None-safe, never raises."""
    ext = factory.acquire()
    if ext is None:
        return "error", ""
    try:
        status, title = await probe(ext.session, item_id)
    except Exception:
        status, title = "error", ""
    await factory.release(ext, status, title)
    return status, title


async def sweep_frontier(factory, start):
    """Localise the live frontier with concurrent fans (~1 round-trip each). Each fan probes many
    ids at once and looks for the born→absent edge. If the whole fan is still born the seed was a
    stale catalog snapshot (born far above where it claims) — so we jump to the top and widen the
    step, brackets the frontier in a few rounds however stale the seed is, yet stays ~2 round-trips
    in the common fresh case. Fresh each round, so unlike a slow iterative climb it can't go stale."""
    step, points = 600, 28
    base = start
    for _ in range(8):
        ids = [base + i * step for i in range(points)]
        res = await asyncio.gather(*[quick_probe(factory, i) for i in ids])
        status = [s for s, _ in res]                # aligned with ids, ascending
        born = [ids[i] for i in range(points) if status[i] == "born"]
        # Frontier edge = first place TWO ids in a row read absent. Requiring two consecutive means
        # a lone 404 "hole" below the frontier (a deleted/reserved id) can't fool a single probe.
        edge = next((i for i in range(points - 1)
                     if status[i] == "absent" and status[i + 1] == "absent"), None)
        if edge is not None:                        # the fan straddles the frontier → found it
            below = [ids[i] for i in range(edge) if status[i] == "born"]
            return max(below) if below else ids[edge] - step
        if born:                                    # no dead edge in range → frontier is higher
            base, step = max(born), step * 4        # (stale seed) jump to the top and widen
        else:                                       # whole fan absent/error → frontier is lower
            base -= points * step
    return max(base, start)


async def sentinel(factory, item_id, births, skipped, deadline, done, metrics, timers):
    """Watch one id, PIPELINED: launch a probe every ID_SPACING without awaiting the previous,
    so calls/s per sentinel = 1/ID_SPACING regardless of round-trip. Record the birth only if we
    witnessed it flip 404 -> 307 (an id already born when we started is skipped, not timed)."""
    born = asyncio.Event()
    st = {"saw_absent": False, "skipped": False, "hits": 0}
    inflight = set()

    async def one_probe():
        ext = factory.acquire()
        if ext is None:                       # pool momentarily drained — this call doesn't reach the server
            return
        st["hits"] += 1
        t0 = time.time()
        status, title = await probe(ext.session, item_id)
        metrics["probes"] += 1
        metrics["probe_time"] += time.time() - t0
        await factory.release(ext, status, title)
        if status == "absent":
            st["saw_absent"] = True
        elif status == "born" and not born.is_set():
            if st["saw_absent"]:              # witnessed 404 -> 307 = a real birth we can time
                now = time.time()
                births[item_id] = (now, title)
                if timers["first_id"] is None:
                    timers["first_id"] = now
                print(f"[ID ] {stamp(now)}  #{item_id}  {title}", flush=True)
            else:                             # already 307 on our very first look — not a witnessed birth
                st["skipped"] = True
            born.set()

    t_first = time.time()
    while time.time() < deadline and not done.is_set() and not born.is_set():
        if len(inflight) < MAX_INFLIGHT:
            task = asyncio.create_task(one_probe())
            inflight.add(task)
            task.add_done_callback(inflight.discard)
        await asyncio.sleep(ID_SPACING)
    if inflight:                              # let the probe that saw the birth finish recording
        await asyncio.gather(*inflight, return_exceptions=True)

    if st["skipped"]:
        skipped.append(item_id)
    active = max(1e-6, time.time() - t_first)
    if st["hits"]:
        metrics["rates"].append(st["hits"] / active)  # true calls/s this sentinel made to the server


async def catalog_watch(factory, births, catalog_seen, leads, deadline, done, state, timers, metrics):
    """Poll the catalog, PIPELINED: fire a read every CATALOG_POLL WITHOUT awaiting the previous,
    so the effective poll rate stays ~1/CATALOG_POLL regardless of the proxy round-trip — a slow
    proxy can't quietly stretch the interval and inflate the lead. Every successful read's time is
    recorded (metrics["catalog_reads"]) so the RESULT can report the real poll cadence — the
    accuracy of each 'catalog' timestamp — instead of asking you to trust it. A cache-buster keeps
    each read fresh (the catalog is cached)."""

    async def one_read():
        ext = factory.acquire()
        if ext is None:                       # pool momentarily drained — this read doesn't happen
            return
        try:
            r = await ext.session.get(f"{CATALOG}&_={int(time.time() * 1000)}", timeout=8)
            items = r.json().get("items", [])
        except Exception:
            items = []
        await factory.release(ext, "absent", "")
        if not items:
            return
        now = time.time()
        metrics["catalog_reads"].append(now)  # timestamp of every real read → reported poll cadence
        ids = [it.get("id") for it in items if it.get("id")]
        top = max(ids) if ids else 0
        # A rotating exit can serve a stale cached page (top far below the live frontier). The real
        # top only grows, so reject a read implausibly far below what we've seen — it just skips.
        if top and top > state["catalog_top"] - 5000:
            state["catalog_top"] = top
        # A born listing is "in the catalog" the moment the newest catalog id has climbed PAST it —
        # match on that crossing, not on catching the id inside the newest-96 page (at peak that page
        # is ~0.3s wide and slides by faster than we can poll; the crossing test is robust).
        for iid, (t_id, title) in list(births.items()):
            if iid in catalog_seen or top < iid:
                continue
            catalog_seen[iid] = now
            lead = max(0.0, now - t_id)
            leads.append(lead)
            if timers["first_match"] is None:
                timers["first_match"] = now
            print(f"\n  #{iid}  {title}")
            print(f"     by ID   : {stamp(t_id)}")
            print(f"     catalog : {stamp(now)}")
            print(f"     LEAD    : +{lead:.1f}s\n", flush=True)
            if len(leads) >= MATCHES:
                done.set()

    inflight = set()
    while time.time() < deadline and not done.is_set():
        if len(inflight) < MAX_INFLIGHT:
            task = asyncio.create_task(one_read())
            inflight.add(task)
            task.add_done_callback(inflight.discard)
        await asyncio.sleep(CATALOG_POLL)
    if inflight:
        await asyncio.gather(*inflight, return_exceptions=True)


async def progress(factory, births, catalog_seen, sentinels, state, metrics, deadline, done):
    """Heartbeat so the wait for the slow catalog to climb up to the sentinels never looks hung.
    Tracks probes since the previous tick so the calls/s figure is live, and shows the pool
    (idle/live) so starvation — the killer of the poll rate — is visible as it happens."""
    lowest = sentinels[0]
    last_probes, last_t = metrics["probes"], time.time()
    last_cat = len(metrics["catalog_reads"])
    while time.time() < deadline and not done.is_set():
        await asyncio.sleep(5)
        if done.is_set():
            return
        now = time.time()
        rate = (metrics["probes"] - last_probes) / (now - last_t)
        cat_rate = (len(metrics["catalog_reads"]) - last_cat) / (now - last_t)
        last_probes, last_t = metrics["probes"], now
        last_cat = len(metrics["catalog_reads"])
        active = max(1, len(sentinels) - len(births))
        gap = lowest - state["catalog_top"]
        where = f"catalog ~{gap} ids below the sentinels" if gap > 0 else "catalog is reaching them now"
        rtt = metrics["probe_time"] / metrics["probes"] * 1000 if metrics["probes"] else 0
        err(f"… {len(births)}/{len(sentinels)} timed | {len(catalog_seen)} matched | {where} | "
            f"pool {len(factory._idle)}idle/{factory.live_count()}live | "
            f"{rate:.0f}/s = {rate / active:.1f}/s per sentinel | catalog {cat_rate:.1f}/s | {rtt:.0f}ms rtt")


async def main():
    require_proxy()
    t_start = time.time()
    factory = SessionFactory(target=POOL, create_concurrency=POOL)
    err(f"warming {PREWARM} sessions through the proxy (pool fills to {POOL} in the background)…")
    await factory.prewarm(PREWARM)
    if factory.live_count() == 0:
        err("could not create sessions — check the proxy.")
        return
    replenisher = asyncio.create_task(factory.replenish_loop())
    t_warm = time.time()

    boot = RotatingSession(factory)
    start = await newest_catalog_id(boot)
    frontier = await sweep_frontier(factory, start)
    t_frontier = time.time()
    sentinels = [frontier + MARGIN + i * SENT_STEP for i in range(SENTINELS)]
    err(f"frontier ~{frontier} (catalog ~{frontier - start} ids behind) — swept in {t_frontier - t_warm:.0f}s")
    err(f"placing {SENTINELS} sentinels at +{MARGIN}..+{MARGIN + SENTINELS * SENT_STEP} above it, "
        f"pipelined ~{1 / ID_SPACING:.1f} calls/s each — stop at {MATCHES} matches…\n")

    births, catalog_seen, skipped, leads = {}, {}, [], []
    state = {"catalog_top": start}
    metrics = {"probes": 0, "probe_time": 0.0, "rates": [], "catalog_reads": []}
    timers = {"first_id": None, "first_match": None}
    done = asyncio.Event()
    deadline = time.time() + DURATION

    sent_tasks = [asyncio.create_task(sentinel(factory, s, births, skipped, deadline, done, metrics, timers)) for s in sentinels]
    cat_task = asyncio.create_task(catalog_watch(factory, births, catalog_seen, leads, deadline, done, state, timers, metrics))
    prog_task = asyncio.create_task(progress(factory, births, catalog_seen, sentinels, state, metrics, deadline, done))

    async def coordinator():
        """Stop cleanly: once every sentinel has resolved (born or skipped), wait for the catalog
        to catch the timed ones — but quit STALL seconds after the last match lands (the catalog
        eventually starts serving stale snapshots and the final few may never arrive), instead of
        idling to DURATION."""
        await asyncio.gather(*sent_tasks, return_exceptions=True)
        if not births:
            done.set()
            return
        last_n, last_change = -1, time.time()
        while not done.is_set() and time.time() < deadline:
            n = len(leads)
            if n >= MATCHES or len(catalog_seen) >= len(births):
                break
            if n != last_n:
                last_n, last_change = n, time.time()
            elif time.time() - last_change > STALL:   # catalog stopped producing matches
                break
            await asyncio.sleep(0.5)
        done.set()

    try:
        await asyncio.gather(cat_task, prog_task, asyncio.create_task(coordinator()), return_exceptions=True)
    finally:
        replenisher.cancel()
        await factory.close()

    avg_rtt = metrics["probe_time"] / metrics["probes"] if metrics["probes"] else 0
    per_sentinel = statistics.median(metrics["rates"]) if metrics["rates"] else 0
    print("\n=== RESULT ===")
    print(f"sentinels timed by ID (real 404->307) : {len(births)}")
    print(f"of those, later seen in catalog        : {len(catalog_seen)}")
    if skipped:
        print(f"(skipped {len(skipped)} already-born at start — raise MARGIN if this is most of them)")
    print(f"probe speed   : {metrics['probes']} probes, avg {avg_rtt * 1000:.0f}ms round-trip, "
          f"pipelined → ~{per_sentinel:.1f} calls/s per sentinel")
    cat_reads = sorted(metrics["catalog_reads"])
    if len(cat_reads) >= 2:
        cat_gap = statistics.median(b - a for a, b in zip(cat_reads, cat_reads[1:]))
        print(f"catalog poll  : {len(cat_reads)} reads, median gap {cat_gap:.2f}s "
              f"→ each 'catalog' timestamp is accurate to ~{cat_gap:.2f}s (not a polling artifact)")
    if timers["first_match"]:
        print(f"timeline      : frontier +{t_frontier - t_start:.0f}s | first birth +{timers['first_id'] - t_start:.0f}s | "
              f"first match +{timers['first_match'] - t_start:.0f}s | done +{time.time() - t_start:.0f}s")
    else:
        print(f"timeline      : frontier +{t_frontier - t_start:.0f}s | no match yet")
    if leads:
        print(
            f"lead (catalog − ID)                    : median +{statistics.median(leads):.1f}s  "
            f"(min {min(leads):.1f} / max {max(leads):.1f})"
        )
        print("\n→ the ID-frontier approach detected each listing that many seconds BEFORE the catalog.")
    else:
        print("no timed sentinel reached the catalog in the window — busy hours are faster; try a longer DURATION.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        err("\nstopped.")
