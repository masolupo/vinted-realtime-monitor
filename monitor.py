"""
monitor.py — Read every new Vinted listing (id + title) in real time.

A listing is reachable by its item ID shortly after it is created — before it shows up in
the public search catalog:

    GET /items/{id}   ->   307 redirect (born)   |   404 (not yet)

The 307's Location header is `/items/{id}-{slug}`, and the slug is the title — so we get the
id and the title with no detail call and no account.

The catch we learned the hard way: an id appears in the sequence a little *before* its page
becomes reachable, and the 404->307 flip happens out of order and with a variable delay
(some listings take tens of seconds; a brand-new one may even answer 200 with no title yet).
So we don't march a cursor and give up on stubborn 404s — we keep every not-yet-born id in a
`pending` set and re-probe it (every PROBE_INTERVAL) until it flips to 307 (then we print it)
or until it has been pending longer than MAX_AGE (a real hole / abandoned draft). Nothing is
ever dropped by distance, only by time, so a late-appearing listing has the full window to
show up.

To avoid anti-bot bans, probes borrow sessions from the self-replenishing pool in
extractor.py (each retired after a random 100-200 uses and replaced), and the Vinted
primitives live in vinted.py. An independent heartbeat reports throughput and pool health,
and says so loudly if the chase ever stalls, so a hang is never silent.

Educational / research use. A proxy is REQUIRED (see .env.example). Read the DISCLAIMER.

Usage:
    python monitor.py                                # defaults: 200 sessions, 150 concurrency
    VINTED_DOMAIN=www.vinted.fr python monitor.py
    SESSIONS=100 CONCURRENCY=100 python monitor.py   # lighter, for a small/slow proxy
"""

import asyncio
import os
import statistics
import time

from dotenv import load_dotenv

from extractor import SESSIONS, RotatingSession, SessionFactory
from vinted import DOMAIN, err, find_frontier, newest_catalog_id, probe, require_proxy

load_dotenv()

# The chase keeps a `pending` set of not-yet-born ids and re-probes them on an interval.
#   LOOKAHEAD      how far above the highest birth seen to enroll new ids to watch.
#   PROBE_INTERVAL min seconds between re-probes of the same id — a 404 needn't be rechecked
#                  every cycle; probing the whole pending set each cycle just saturates the
#                  proxy. Also the worst-case detection delay for a birth.
#   MAX_AGE        how long (seconds) to keep re-probing an id before giving up on it — the
#                  only thing that ever abandons an id, so late-appearing listings aren't lost.
#   PENDING_CAP    safety bound on the pending set (if the proxy can't keep up, drop the oldest).
CONCURRENCY = int(os.getenv("CONCURRENCY", "150"))
LOOKAHEAD = int(os.getenv("LOOKAHEAD", "250"))
PROBE_INTERVAL = float(os.getenv("PROBE_INTERVAL", "1.0"))
MAX_AGE = float(os.getenv("MAX_AGE", "90"))
PENDING_CAP = int(os.getenv("PENDING_CAP", "5000"))
INTERVAL = float(os.getenv("INTERVAL", "0.05"))
HEARTBEAT = float(os.getenv("HEARTBEAT", "5"))  # seconds between stderr status lines
GAVEUP_LOG = os.getenv("GAVEUP_LOG")  # optional: append abandoned ids to this file to audit coverage


class Stats:
    """Shared state the chase loop updates and the heartbeat task reads — so status is
    reported even while the loop is busy, and a stall can be detected from outside."""

    def __init__(self, top):
        self.total = 0          # listings printed since start
        self.top = top          # frontier estimate (highest birth seen)
        self.gaveup = 0         # ids abandoned after MAX_AGE (holes / drafts; real misses ⊆ this)
        self.born = 0           # since last heartbeat
        self.absent = 0
        self.err = {}           # reason -> count, since last heartbeat
        self.lat = []           # successful-probe durations (seconds) since last heartbeat
        self.pending = 0        # size of the pending set right now
        self.cycle_start = time.time()
        self.in_cycle = False   # True while awaiting a cycle's probes


async def heartbeat_loop(stats, factory):
    """Print a status line every HEARTBEAT seconds — independently of the chase loop, so it
    fires even if a cycle hangs. If the current cycle has been in flight too long, say so
    loudly instead of going silent."""
    last_total, last_t = stats.total, time.time()
    while True:
        await asyncio.sleep(HEARTBEAT)
        now = time.time()
        dt = (now - last_t) or 1e-9
        rate = (stats.total - last_total) / dt
        lat = stats.lat
        lattxt = f" | probe ~{statistics.median(lat):.2f}s med" if lat else ""
        head = (
            f"… {stats.total} read | {rate:.0f} listings/s | frontier ~#{stats.top} | "
            f"pool {factory.live_count()}/{SESSIONS} (+{factory.created} -{factory.retired}){lattxt}"
        )
        if stats.in_cycle and (now - stats.cycle_start) > max(10.0, 2 * HEARTBEAT):
            err(
                head + f" | ⚠ STALLED {now - stats.cycle_start:.0f}s — a cycle of "
                f"{stats.pending} probes hasn't returned (proxy hung / all sessions dead?)"
            )
        else:
            errtxt = ""
            if stats.err:
                total_err = sum(stats.err.values())
                worst = ", ".join(
                    f"{c}×{n}" for c, n in sorted(stats.err.items(), key=lambda kv: -kv[1])[:3]
                )
                errtxt = f" | ERR {total_err} ({worst})"
            extra = f" | pending {stats.pending}"
            extra += f" | gave-up {stats.gaveup}" if stats.gaveup else ""
            err(head + f" | last {HEARTBEAT:.0f}s: born {stats.born} absent {stats.absent}{errtxt}{extra}")
        last_total, last_t = stats.total, now
        stats.born = stats.absent = 0
        stats.err = {}
        stats.lat = []


async def watch(factory):
    """Chase the live ID frontier and print every new listing (id + title) as it is born.

    `top` is the highest birth seen (our frontier estimate). We enroll every id from just
    above the last-enrolled one up to top + LOOKAHEAD into `pending`, and re-probe each pending
    id every PROBE_INTERVAL: a 307 is a birth (printed once, pushes `top` up); anything else
    (404 not-yet-born, 200 still-incomplete, a transient error) keeps the id pending to try
    again. An id is only ever abandoned once it has been pending longer than MAX_AGE — by
    time, never by distance — so a listing that appears late or out of order is still caught.

    Every probe checks a session out of the factory and returns it, so banned IPs are retired
    and replaced under us without stalling the chase."""
    boot = RotatingSession(factory)
    start = await newest_catalog_id(boot)
    top = await find_frontier(boot, start)
    err(f"frontier found: {top}  (catalog was ~{top - start} ids behind)")
    err("reading new listings in real time — id + title as each is born…\n")

    sem = asyncio.Semaphore(CONCURRENCY)
    stats = Stats(top=top)

    async def probe_one(item_id):
        async with sem:
            ext = factory.acquire()
            if ext is None:
                return item_id, ("error", "no-session")
            t0 = time.time()
            status, reason = await probe(ext.session, item_id)
            if status in ("born", "absent"):
                stats.lat.append(time.time() - t0)
            # `reason` carries the title on 'born' and the error code on 'error'. release()
            # keeps the session unless it looks banned (403/429).
            await factory.release(ext, status, reason)
            return item_id, (status, reason)

    hb_task = asyncio.create_task(heartbeat_loop(stats, factory))

    pending = {}          # id -> time first enrolled (still not born)
    last_probe = {}       # id -> time last probed (to space re-probes by PROBE_INTERVAL)
    emitted = set()       # ids already printed
    next_id = top + 1     # lowest id not yet enrolled into pending
    gaveup_file = open(GAVEUP_LOG, "a", buffering=1) if GAVEUP_LOG else None

    try:
        while True:
            now = time.time()
            # Enroll new ids up to the lookahead horizon above the highest birth seen.
            horizon = top + LOOKAHEAD
            while next_id <= horizon:
                if next_id not in emitted and next_id not in pending:
                    pending[next_id] = now
                next_id += 1
            # Give up on ids pending longer than MAX_AGE (real holes / abandoned drafts).
            for i in [i for i, t0 in pending.items() if now - t0 >= MAX_AGE]:
                del pending[i]
                last_probe.pop(i, None)
                stats.gaveup += 1
                if gaveup_file:
                    gaveup_file.write(f"{i}\n")
            # Safety: if the pending set outgrows the cap (proxy can't keep up), drop oldest.
            if len(pending) > PENDING_CAP:
                for i in sorted(pending, key=pending.get)[: len(pending) - PENDING_CAP]:
                    del pending[i]
                    last_probe.pop(i, None)
                    stats.gaveup += 1
                    if gaveup_file:
                        gaveup_file.write(f"{i}\n")

            stats.pending = len(pending)
            # Probe only the ids not checked in the last PROBE_INTERVAL. A never-probed id
            # (last_probe defaults to 0) is due immediately; a 404 we saw a moment ago waits.
            # This is what keeps us from hammering the whole pending set every cycle.
            due = [i for i in pending if now - last_probe.get(i, 0.0) >= PROBE_INTERVAL]
            for i in due:
                last_probe[i] = now
            stats.cycle_start = time.time()
            stats.in_cycle = True
            results = await asyncio.gather(*(probe_one(i) for i in due))
            stats.in_cycle = False

            for item_id, (status, reason) in results:
                if status == "born":
                    if pending.pop(item_id, None) is not None:
                        last_probe.pop(item_id, None)
                        emitted.add(item_id)
                        print(f"{time.strftime('%H:%M:%S')}  #{item_id}   {reason}", flush=True)
                        stats.total += 1
                        stats.born += 1
                        if item_id > top:
                            top = item_id
                elif status == "absent":
                    stats.absent += 1
                else:  # 200 / net / no-session / … → keep pending, retry next cycle
                    stats.err[reason] = stats.err.get(reason, 0) + 1
            stats.top = top

            # Bound the emitted set's memory: once it's large, forget ids well below the
            # oldest still-pending id (they're long done and won't be re-enrolled).
            if len(emitted) > PENDING_CAP:
                floor = (min(pending) if pending else top) - LOOKAHEAD
                emitted = {i for i in emitted if i >= floor}

            if INTERVAL:
                await asyncio.sleep(INTERVAL)
    finally:
        hb_task.cancel()
        if gaveup_file:
            gaveup_file.close()


async def main():
    require_proxy()
    err(f"domain: {DOMAIN} | target sessions: {SESSIONS} | probe concurrency: {CONCURRENCY}")

    factory = SessionFactory()
    err(f"pre-warming {SESSIONS} sessions through the proxy (a few seconds)…")
    await factory.prewarm(SESSIONS)
    if factory.live_count() == 0:
        err("Could not create any session (proxy or network issue).")
        return
    err(f"ready: {factory.live_count()}/{SESSIONS} sessions warm — starting.\n")

    replenisher = asyncio.create_task(factory.replenish_loop())
    try:
        await watch(factory)
    finally:
        replenisher.cancel()
        await factory.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        err("\nstopped.")
