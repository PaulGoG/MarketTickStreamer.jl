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

## Ranked recommendation (solo researcher, few symbols, live + bulk historical)

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
