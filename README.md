# Vinted Real-Time Monitor — reading new listings by item ID

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-educational-orange.svg)

> Read **every new Vinted listing (id + title) the instant it's created** — seconds
> before it reaches the public search catalog. A small, self-contained proof of concept,
> and probably how the fastest monitors (Souk & co.) feel "instant".

No account, no purchases, no cracked authentication or third-party secrets. One idea:
**watch the item-ID frontier, not the search index** — plus the engineering it takes to do
that without getting your IP banned.

![Latency benchmark — the ID-frontier path detected each new listing a median of +10.2s before the public catalog; the catalog was polled every ~0.74s, so the lead is not a polling artifact](docs/lead.png)

---

## The idea

Almost every Vinted monitor polls the search endpoint:

```
GET /api/v2/catalog/items?order=newest_first
```

The problem: **that index is already several seconds stale.** No matter how fast you poll it,
you can't go below that floor — everyone polling the catalog looks at the same delayed
picture.

But Vinted assigns **incrementing IDs** to new listings, and a listing is reachable by
its ID within a second or so of being created — well before it hits the search index:

```
GET /items/{id}   →   307 redirect   (born)   |   404   (not yet)
```

The 307's redirect target (`/items/{id}-{slug}`) even carries the **title** in the slug,
for free and without authentication:

```
GET /items/9361111097
→ 307  Location: /items/9361111097-maleta-viaje-mano
                              └──────────┬──────────┘
                                    the title
```

So instead of polling a stale list, this watches the **ID frontier** and reads each
listing — id and title — at birth.

> **⚠️ The domain does not filter what you see.** `www.vinted.it`, `www.vinted.fr`,
> `www.vinted.de` all surface the **same worldwide stream**: item IDs are a single global
> sequence, not partitioned by country. Picking a national TLD does **not** limit you to
> that country's listings — you see everything, everywhere. The country / TLD / proxy
> matching (see [Quick start](#quick-start)) is only about *being served at all* — Vinted's
> anti-bot rejects mismatched IP↔domain requests. Avoid `www.vinted.com`: it works, but it
> frequently serves **cached / stale** responses — use a national TLD for a live feed.
> (Confirmed through extensive testing.)

---

## How it works

Three moving parts, one per file:

**1. Find the frontier** (`vinted.py`). Read the newest id from the catalog (known to
exist, but stale — and read it a few times with a cache-buster so a badly stale CDN snapshot
can't send us off course), then a concurrent binary search with `GET /items/{id}` locates
the highest id that currently returns a 307. That's the live frontier.

**2. Chase it *patiently*** (`monitor.py`). The ids just above are still `404` (not born
yet). We keep every not-yet-born id in a `pending` set and re-probe it on an interval until
it flips to `307` (**print it**) — or until it has been pending longer than `MAX_AGE`, at
which point it's a genuine hole (an abandoned draft / deleted slot) and we drop it. The
catch we learned the hard way: ids appear a little *before* their page is reachable, and
the `404→307` flip happens **out of order** and with a variable delay. Marching a cursor
and giving up on stubborn 404s loses those late-appearing listings; a time-based patient
set catches them.

**3. Don't get banned** (`extractor.py`). Vinted's anti-bot bans an IP that makes too many
requests, so a long-lived session is a dead end. Instead we run a **self-replenishing pool
of short-lived sessions** (the same idea as a rotating fleet of browser tabs): each session
is retired after a random 100–200 uses and replaced, a background loop keeps the pool
topped up, and any session that gets a `403/429` is retired immediately. Sessions are
handed out by exclusive checkout, so one is never closed mid-request under another probe.

The title comes free from the 307 slug — category, price and photos are enrichment you'd
add on top, and enrichment (not detection) is where the real bandwidth cost lives.

---

## How complete is it?

In a measured run it read **~99.9%** of new listings. The tool tracks everything it gives
up on (set `GAVEUP_LOG=gaveup.log`), and `check.py` re-probes that list to classify it:

```
$ python check.py gaveup.log
=== SUMMARY (audit of ABANDONED ids only — NOT a coverage figure) ===
checked          : 102
404  (real holes): 34    — never a listing (draft/deleted), correctly skipped
307  (MISSES)    : 10    — real listings that appeared after MAX_AGE
other            : 58    — anti-bot 200 pages masking a 404 (a browser shows "not found")
```

Read that carefully: those numbers are **only the ids the monitor gave up on** (~1.4% of
everything it read that run), *not* a coverage percentage. Of them, just **10** were real
listings that surfaced *after* the patience window — so the real miss rate is `10 ÷ ~7000
read ≈ 0.14%`, i.e. **~99.9% coverage**. Raise `MAX_AGE` to catch more of the late ones; the
rest are genuine holes (ids that never became a public listing).

---

## Measuring the lead

The lead isn't a guess — `latency.py` measures it. It puts sentinels on the ids about to be
born and watches each one *both* ways at once — by ID and in the catalog — then prints the gap
for each, plus a median:

```
$ python latency.py
  #9424054263  gap cropped tee xl
     by ID   : 00:48:03.517
     catalog : 00:48:14.975
     LEAD    : +11.5s
…
=== RESULT ===
sentinels timed by ID (real 404->307) : 7
of those, later seen in catalog        : 6
catalog poll  : 142 reads, median gap 0.74s → each 'catalog' timestamp is accurate to ~0.74s
lead (catalog − ID)                    : median +10.2s  (min 7.7 / max 11.5)
```

The lead ≈ how far the catalog trails the frontier ÷ the current mint rate, so it **depends on
the hour**: around **~6–8s at peak** (fast minting), and *more* off-peak — the run above, late
evening, measured a median +10.2s. Either way the ID path wins by seconds, reproducibly, on your
own proxy.

**Why this is a fair comparison (not a polling artifact).** The catalog's lag is a *server-side
indexing delay*, not something you can poll away: `catalog/items?newest_first` trails the true
frontier by seconds no matter how fast you hit it. `latency.py` still polls it fast — pipelined,
with a cache-buster so every read is fresh — and it **measures and prints its own poll cadence**
(the `catalog poll` line above: a ~0.74s median gap, under 10% of the lead), so you can verify the
granularity instead of trusting it. It also credits the catalog the instant its newest id *crosses*
a sentinel — the earliest reasonable moment, generous to the catalog — so the measured lead is if
anything a slight under-estimate.

---

## What's in here

| File | What it does |
| --- | --- |
| `monitor.py` | The app: chases the frontier, prints `#id  title` live, prints a heartbeat |
| `vinted.py` | The Vinted layer: config, a warmed session, `probe()`, frontier search |
| `extractor.py` | The self-replenishing pool of short-lived sessions (anti-ban) |
| `check.py` | Diagnostic: re-probe a list of ids (or a `GAVEUP_LOG`) and classify 307/404 |
| `latency.py` | Diagnostic: measures the ID path's lead over the catalog, on the same listings |

---

## Quick start

**1. Create a virtual environment and install the dependencies**

```bash
python3 -m venv .venv            # Windows: python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**2. Configure your proxy** (required — see the note below)

```bash
cp .env.example .env
```

Open `.env` and set your proxy and the matching domain:

```bash
HTTP_PROXY=http://user:pass@host:port    # an IP in the same country as the domain
VINTED_DOMAIN=www.vinted.it              # the national TLD for that country
```

**3. Run it**

```bash
python monitor.py                                # defaults: 200 sessions, 150 concurrency
# lighter, for a small or slow proxy (may fall behind at peak hours):
SESSIONS=100 CONCURRENCY=100 python monitor.py
```

Diagnostics and the heartbeat go to **stderr**; the clean `#id  title` stream goes to
**stdout** (so you can pipe it). New listings scroll by like this:

```
14:41:07  #9302874961   blouse rose zara taille s
14:41:07  #9302874962   nintendo ds lite complete
14:41:08  #9302874964   nike air max 90 42
```

**A proxy is required.** Vinted blocks bare IPs quickly, so the tool refuses to start
without `HTTP_PROXY` set — this protects your own address. The proxy's country, the
`VINTED_DOMAIN` TLD, and the request language must all match (an Italian proxy with
`www.vinted.it`, a French proxy with `www.vinted.fr`, …); avoid `www.vinted.com` — it works
but often serves cached / stale data.
If you use a **rotating** proxy, lock it to a single country (a country filter matching the
TLD) — a pool that rotates across countries lands on the wrong geo and Vinted stops
answering. `Accept-Language` is derived automatically from `VINTED_DOMAIN`.

> **Which proxy?** It must support country filtering (match your `VINTED_DOMAIN`). Plain
> **residential** proxies work but Vinted's anti-bot blocks them fairly quickly under sustained
> probing — in practice **mobile (4G/5G) proxies hold up far better**: their IPs are shared over
> carrier-grade NAT among many real users, so the anti-bot is reluctant to ban them (it would
> hit real people too). Any provider with per-country residential or mobile IPs works.

---

## The heartbeat

Every few seconds the tool prints a status line to stderr — this is how you see whether
it's keeping up, and it never goes silent (if a cycle stalls, it says so):

```
… 8933 read | 104 listings/s | frontier ~#9370629097 | pool 200/200 (+434 -254) | probe ~0.6s med | last 5s: born 518 absent 31 | ERR 16 (200×7, net×6) | pending 335 | gave-up 0
```

- **`pool 200/200`** — live sessions vs target. If it drains, raise `SESSIONS` / lower
  `CONCURRENCY`; if you see `no-session` errors, the pool is starving.
- **`probe ~0.6s med`** — median probe latency. Your throughput is `CONCURRENCY ÷ latency`.
- **`gave-up`** — ids abandoned after `MAX_AGE` (mostly holes; the audit above shows how
  many were real). This is your coverage gauge.
- **`ERR (403×…)`** — real anti-bot bans. If these climb, lower `MAX_USES`.

---

## Tuning

Everything is an env var (defaults in parentheses):

| Var | What it does |
| --- | --- |
| `SESSIONS` (200) | Target size of the session pool. Keep it ≥ `CONCURRENCY`. |
| `CONCURRENCY` (150) | Max probes in flight. Throughput ≈ this ÷ latency. |
| `PROBE_INTERVAL` (1.0) | Min seconds between re-probes of the same id (spares the proxy). |
| `MAX_AGE` (90) | How long to keep re-probing an id before giving up on it. |
| `PROBE_TIMEOUT` (2) | Per-probe timeout (~3–6× the typical latency). |
| `MAX_USES_MIN/MAX` (100/200) | Random per-session request budget before retirement. Lower it if `403`s appear; raise it if the pool drains. |
| `CREATE_CONCURRENCY` (40) | How many sessions to warm up at once. |
| `GAVEUP_LOG` (unset) | Append abandoned ids to this file, to audit coverage with `check.py`. |

---

## The catch — detection is cheap, scale is an infrastructure game

This repo does **detection**: id + title, no account, in real time. Two real walls make doing
it *completely* and *continuously* an infrastructure problem, not a cleverer endpoint:

- **Throughput.** New ids are minted globally at up to ~140/second at peak. To read *all*
  of them you must out-probe that rate — `probes/second = CONCURRENCY ÷ latency` — which at
  residential-proxy latencies means a big pool of concurrent sessions.
- **Session churn.** Each session survives only ~100–200 requests before it must be retired
  and replaced, and replacing one costs a full cookie warm-up. Sustaining the pool means
  minting fresh sessions continuously; if the proxy can't warm them fast enough, the pool
  drains and coverage drops.

The other half — **enrichment** (description, price, size, condition, photos, seller) — lives
behind Vinted's **authenticated** per-item endpoint, which needs a logged-in account and is
aggressively rate-limited. So the "full package" (real-time detection *plus* rich data on
every listing) isn't a smarter trick: it's **two infrastructures** — a proxy pool for
detection, a fleet of accounts for depth.

*(Running that at scale — especially farming accounts — is squarely against Vinted's Terms of
Service and out of scope for this educational proof of concept. The point is to map where the
cost actually is, not to build the abusive version.)*

---

## Disclaimer

Educational and research use only. This project is **not affiliated with, endorsed by, or
connected to Vinted or any third-party tool**.

It uses Vinted's **own internal, undocumented endpoints** (the item-page route and the
internal `catalog/items` API) — these are *publicly reachable*, but they are **not a
public/sanctioned API** (Vinted's only official API is the allowlisted Vinted Pro). Their
behavior was mapped by **observation**, not by decompiling or cracking any authentication.
It reaches them through **automated id enumeration and browser / anti-bot impersonation**,
which crosses Vinted's Terms of Service.

Any resemblance to how fast hosted monitors work is an **educated guess from observed
behavior**, not a leak: no third-party system was reverse-engineered, and this repo contains
**no third-party keys, channels, or credentials**.

This is a proof of concept meant to *study* the technique, not a tool to run at scale: don't
mass-scrape, spam, or automate purchases. Use responsibly and at your own risk.

## License

MIT — see [LICENSE](LICENSE).
