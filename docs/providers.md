# Tick-data provider landscape

Verified 2026-07-30/31 against providers' own pricing pages, docs, and (for
Databento) their production pricing API. Prices change; re-verify before
committing money. Flagged-unverified items are listed at the end.

## Comparison — tick-by-tick US equities

| Provider | Live ticks (real-time) | Historical ticks | Cost for this project | Julia feasibility |
|---|---|---|---|---|
| **Alpaca** | Free: IEX feed, 30 symbols, WS+JSON. $99/mo ("Algo Trader Plus"): consolidated SIP, unlimited symbols | Free: SIP trades since 2016 via REST (minus trailing 15 min), paginated 10k/page, 200 req/min | **$0 → $99/mo** | Trivial (implemented here) |
| **Massive** (ex-Polygon.io, rebranded 2025-10) | $199/mo "Advanced": SIP WS. $79/mo "Developer": 15-min delayed | $79/mo+: S3 flat files — one `csv.gz`/day of the full consolidated tape, ns timestamps, 10 yr (Advanced: to 2003) + REST (50k/page) | **$79–199/mo** | Trivial (WS ms-epoch ints; flat files via any S3 client) |
| **Databento** | $199/mo flat: unlimited live, but raw TCP + binary DBN protocol, no WebSocket; EQUS.MINI feed (no venue fees) covers only a subset of prints; Nasdaq TotalView live barred for individuals | Usage-based: Nasdaq ITCH trades $6/GB ≈ **$10–11 per symbol-year** (AAPL); CSV/JSON over HTTPS Basic-auth; $125 intro credit | **~$50–110 one-off** for 5–10 symbol-years historical; $199/mo if live needed | Historical trivial; live needs custom TCP/CRAM/DBN client (open spec, 48-byte fixed records — doable) |
| **ThetaData** | $160/mo "Stocks Pro": near-complete trade stream (Nasdaq Basic feed, not SIP) via local Java terminal | Tick history to 2012 (UTP) / 2017 (CTA), unlimited requests | **$160/mo** | Localhost REST/WS + JSON, but mandatory Java sidecar |
| **Finnhub** | Free: real-time trades WS, 50 symbols — but feed provenance undocumented and community-reported quality issues | $49.99/mo+: consolidated SIP tick REST (T+0 EOD), 5–30 yr by tier | $0 → $50/mo | Trivial |
| **Intrinio** | $150/mo "Individual": derived/predicted BBO feed (not true prints); real feeds Enterprise-only ($1,250/mo+) | Consolidated tape tick history on all plans from $150/mo | $150/mo+ | WS is custom binary (decodable); REST fine |
| **EODHD** | WS exists; source feed/delay undisclosed | REST tick API to 2008, $99.99/mo all-in-one | $99.99/mo | Trivial, but provenance weak — validate first |
| **Tiingo** | IEX WS (ns timestamps) — but since 2025-02 real trade levels require a signed IEX Exchange agreement | **None at tick level** (1-min bars only) — disqualifying | $30/mo | Trivial but pointless without history |
| **IBKR** | ~$1.50–10/mo subscriptions, true consolidated trades — capped at ~3 simultaneous tick streams | 1,000 ticks/request — bulk backfill impractical | ~$10/mo | Jib.jl exists (TWS sidecar) |
| **Twelve Data** | Second-resolution last-price ticker only — **not tick data** at any tier | None | — | — |
| **IEX Cloud** | **Shut down 2024-08-31.** IEX Exchange still publishes free T+1 HIST pcaps of its own tape (needs a TOPS/pcap parser) | — | — | — |

## Free-first stack (verified 2026-07-31; latency/recency not required)

For science rather than trading, delayed and T+1 data are as good as live —
which unlocks a fully free tier most comparisons ignore:

1. **Nasdaq TotalView-ITCH full-day files** — `emi.nasdaq.com/ITCH/`: whole
   Nasdaq days of raw order-level ITCH 5.0 (every add/cancel/execute,
   nanosecond stamps, 3.5–18 GB/day gzipped), free, no registration,
   including recent 2026 dates. The same stream LOBSTER charges £7,499/yr
   for, minus the reconstruction. Cost: a fixed-layout big-endian binary
   parser (~20 message types; `TotalViewITCH.jl` exists as a starting point).
2. **IEX HIST** — `iextrading.com/api/1.0/hist?date=YYYYMMDD`: the only
   free *unbroken multi-year* US equity tick archive — T+1 pcap of IEX's own
   tape (TOPS/DEEP, µs stamps, ~14 GB/day) back to late 2016, no auth.
   Cost: pcap + IEX-TP + TOPS parser (fixed little-endian structs).
3. **Crypto bulk + live** — Binance `data.binance.vision` (full trade
   history CSV dumps + free live trade WS), Kraken full-history ZIP (every
   trade since inception), Bybit `public.bybit.com`, Tardis.dev free
   first-day-of-each-month across exchanges. Unlimited scale, plain CSV/JSON,
   zero auth — the methodology sandbox.
4. **Alpaca free tier** — the US-equities live loop: real-time IEX WS +
   15-min-delayed SIP WS (`v2/delayed_sip`, 30 symbols; free-tier
   availability is documented but worth a one-minute auth test) + free
   historical SIP REST since 2016. Already implemented in this package.
5. **NYSE Daily TAQ + LOBSTER samples** — free full-market consolidated-tape
   sample days (`ftp.nyse.com`, ~2 GB pipe-CSV/day) and nanosecond LOB
   sample CSVs — reference data for validating parsers and pipelines.
6. **Dukascopy `.bi5`** — decades of free FX/CFD quote ticks over
   unauthenticated HTTP (LZMA, 20-byte records; trivial to decode) — for
   long-memory/scaling studies where quotes suffice.

Paid escalation, in order of value: **Databento** usage-based (start on the
$125 signup credit; ~$10/symbol-year Nasdaq trades) → **FirstRate Data**
($19.95–49.95/ticker one-off, 15-year tick archives) → subscriptions
(Alpaca $99/mo live SIP; Massive $79/mo flat files; Kibot $5,940 one-time
full-universe tick archive since 2009 incl. delisted).

Not viable: SEC MIDAS (aggregates only), WRDS/TAQ (institution-gated),
IEX Cloud (dead since 2024-08).

## Ranked recommendation (if real-time live streaming matters)

1. **Alpaca free tier now, $99/mo SIP when live fidelity matters** — the only
   $0 complete loop: real-time IEX ticks + free historical SIP REST through
   the endpoints this package already implements and tests. Upgrade swaps
   `feed = "iex"` → `"sip"` in config; nothing else changes.
2. **Databento for bulk historical** — the cheapest research-grade tape by an
   order of magnitude (~$10/symbol-year, exchange-direct ns provenance,
   plain HTTPS + CSV). Ideal companion to Alpaca live capture for
   cross-validation; skip their live product (TCP/DBN, $199/mo flat).
3. **Massive Advanced ($199/mo)** — best ergonomics if budget allows one flat
   fee: SIP WebSocket + full-tape daily flat files. The $79 Developer tier
   (delayed live, 10 yr flat files) is the value play for mostly-offline work.

**Crypto sandbox ($0)**: Binance trade WS (`btcusdt@trade`, no auth) + free
bulk historical trade dumps at data.binance.vision give an unthrottled 24/7
tick flux with exact tape for cross-validation — the right substrate for
stress-testing streaming estimators before spending on equities feeds. Mind
the microstructure differences (no consolidated tape, halts, or equity
condition semantics). A `BinanceProvider <: AbstractProvider` is a natural
next adapter.

## Flagged unverified

Massive Business-tier pricing (sales-gated); Massive per-account WS
connection limit; IEX volume share (~2–3%, external estimate); Databento
GB definition (±7% on estimates) and TotalView live venue pass-through fees;
Finnhub free-stream feed identity and bulk-download tier availability;
ThetaData middle stock-tier price; EODHD WS provenance and rate-limit
wording; Tiingo/IEX agreement mechanics for individuals.

## Key sources

alpaca.markets/data · docs.alpaca.markets (stocktrades-1, streaming-market-data) ·
massive.com/pricing (301 target of polygon.io/pricing) · massive.com/blog/polygon-is-now-massive ·
massive.com/docs (websocket/stocks/trades.md, flat-files/stocks/trades.md) ·
databento.com/pricing + hist.databento.com/v0/metadata.list_unit_prices ·
github.com/databento/dbn · thetadata.net/pricing · finnhub.io/pricing ·
tiingo.com/about/pricing · iex.io/resources/trading/fee-schedule ·
twelvedata.com/pricing · interactivebrokers.com/en/pricing/market-data-pricing.php ·
eodhd.com/pricing · data.binance.vision
