"""
extractor.py — a self-replenishing pool of short-lived Vinted sessions.

Vinted's anti-bot can ban an IP that makes too many requests, so instead of one long-lived
session we keep many shorter-lived ones: each Extractor is retired after a random 100-200
uses and replaced, and SessionFactory keeps the pool topped up from a background loop. It's
the classic rotating-fleet-of-short-lived-sessions pattern, adapted to id-frontier probing.

Sessions are handed out by *exclusive checkout* (acquire → release): a session is only ever
used by one probe at a time, so it can never be closed while another probe is mid-request on
it — the race that would otherwise hang the whole run.
"""

import asyncio
import os
from collections import deque
from random import randint

from dotenv import load_dotenv

from vinted import err, make_session

load_dotenv()

# Target warm sessions kept in the pool, how many to warm up at once, and the per-session
# use budget before retirement (the anti-ban trick). Sustained throughput is ultimately
# capped by mint-rate × uses-per-session — see the heartbeat's pool figure.
SESSIONS = int(os.getenv("SESSIONS", "200"))
CREATE_CONCURRENCY = int(os.getenv("CREATE_CONCURRENCY", "40"))
# Per-session use budget, drawn at RANDOM per session (randint below). Two reasons for the
# random spread: (1) if every session shared one fixed budget they'd all be created together
# and hit it together, retiring in one synchronized wave that empties the pool — staggered
# budgets make them die a few at a time; (2) a wider range spreads those deaths even further.
# Higher budgets also mean fewer retirements, so the pool needs fewer (costly) warm-ups to
# stay full — raise these if the pool drains, lower them if you start seeing 403/429 bans.
MAX_USES_MIN = int(os.getenv("MAX_USES_MIN", "100"))
MAX_USES_MAX = int(os.getenv("MAX_USES_MAX", "200"))


class Extractor:
    """One warmed session with its own cookies, retired after a handful of uses so no
    single IP is hammered into a ban."""

    __slots__ = ("session", "used", "max_uses")

    def __init__(self, session):
        self.session = session
        self.used = 0
        self.max_uses = randint(MAX_USES_MIN, MAX_USES_MAX)

    def spent(self):
        return self.used >= self.max_uses


class SessionFactory:
    """A self-replenishing pool of Extractors. A background loop keeps ~target sessions
    warm; each probe checks one out via acquire() and returns it via release(), which
    retires it once it is spent or has errored — so an IP that gets banned heals
    automatically instead of killing the run."""

    def __init__(self, target=SESSIONS, create_concurrency=CREATE_CONCURRENCY):
        self.target = target
        self._idle = deque()    # Extractors available for checkout
        self._busy = 0          # Extractors currently checked out
        self._inflight = 0      # sessions currently warming up
        self._sem = asyncio.Semaphore(create_concurrency)
        self._tasks = set()     # strong refs to in-flight spawn tasks (else they can be GC'd)
        self.created = 0
        self.retired = 0

    def live_count(self):
        """Sessions that exist right now (idle + checked out)."""
        return len(self._idle) + self._busy

    async def _spawn(self):
        async with self._sem:
            session = await make_session()
        if session:
            self._idle.append(Extractor(session))
            self.created += 1

    async def prewarm(self, n):
        """Create n sessions before the run starts, concurrently (capped by the semaphore),
        reporting progress so a slow proxy doesn't look like a hang."""
        tasks = [asyncio.create_task(self._spawn()) for _ in range(n)]
        done = 0
        for coro in asyncio.as_completed(tasks):
            await coro
            done += 1
            if done % 10 == 0 or done == n:
                err(f"  warming sessions… {self.live_count()}/{n} ready")

    def acquire(self):
        """Check out an idle session for exclusive use. None if none is free right now
        (the replenisher is catching up) — the caller just retries next cycle."""
        if not self._idle:
            return None
        self._busy += 1
        return self._idle.popleft()

    BAN_CODES = ("403", "429")

    async def release(self, ext, status, reason=""):
        """Return a checked-out session. Retire it (close) if it's spent, or if the response
        looks like a real ban (HTTP 403/429). Everything else keeps the session: a 'net'
        timeout is usually the proxy saturating, and a 200 is just an item that isn't a
        redirect yet — neither means the IP is dead, so we keep it and retry the id later.
        Exclusive checkout means no other probe holds it, so closing here is safe."""
        self._busy -= 1
        ext.used += 1
        banned = status == "error" and reason in self.BAN_CODES
        if banned or ext.spent():
            try:
                await ext.session.close()
            except Exception:
                pass
            self.retired += 1
        else:
            self._idle.append(ext)

    async def replenish_loop(self):
        """Keep the pool topped up to target, spawning the missing sessions concurrently."""
        while True:
            deficit = self.target - self.live_count() - self._inflight
            for _ in range(max(0, deficit)):
                self._inflight += 1
                task = asyncio.create_task(self._spawn_tracked())
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            await asyncio.sleep(0.1)

    async def _spawn_tracked(self):
        try:
            await self._spawn()
        finally:
            self._inflight -= 1

    async def close(self):
        for ext in list(self._idle):
            try:
                await ext.session.close()
            except Exception:
                pass


class RotatingSession:
    """Adapter that looks like an AsyncSession's .get() to the frontier-search code in
    vinted.py, but checks out a fresh session from the factory each call — so the one-off
    boot probes are spread across the pool instead of burning a single IP."""

    def __init__(self, factory):
        self.factory = factory

    async def get(self, url, **kwargs):
        ext = self.factory.acquire()
        if ext is None:
            raise RuntimeError("no live session")
        try:
            r = await ext.session.get(url, **kwargs)
        except Exception:
            await self.factory.release(ext, "error", "net")  # boot timeout → keep the session
            raise
        await self.factory.release(ext, "born")  # any response → keep (only spend/ban retires)
        return r
