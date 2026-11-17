# Golden vendor-file fixtures

Both fixtures hold vendor bytes as published, cut by a generator in this
directory. The test in `test/vendor.jl` checks what the readers decode from
them against facts of the day that do not come from the readers (session
hours, AAPL's 100-share round lot) and, where the local reference exists,
against the consolidated tape's (SIP) record of the same executions: AAPL on
2025-12-09, matched print by print on price, size and nearest time.

## `sip_AAPL_20251209.toml` (local only)

The SIP prints come from an Alpaca historical-trades backfill with
`feed = sip`, and Alpaca's terms do not allow its market data to be
redistributed. The reference is therefore written locally from your own
processed tree, `julia test/fixtures/extract_sip_reference.jl
<data_dir>/processed/AAPL/2025-12-09.arrow test/fixtures/sip_AAPL_20251209.toml`,
and the comparison is skipped without it.

## `iex_AAPL_20251209.pcapng` (committed)

Data provided for free by IEX. By accessing or using IEX Historical Data, you
agree to the [IEX Historical Data Terms of Use](https://www.iex.io/legal/hist-data-terms).
The excerpt is IEX's data under those terms, not under the repository's MIT
licence.

| | |
|---|---|
| Source | `20251209_IEXTP1_TOPS1.6.pcap.gz`, IEX HIST (`https://iextrading.com/api/1.0/hist?date=20251209`) |
| Source size, SHA-256 | 8 608 566 865 bytes, `7024b40097bd7d24388c174dacd0cd548633e788c6671caffdb305caaa666757` |
| Content | the first ten captured frames whose TOPS 1.6 segment holds an AAPL Trade Report, verbatim (Ethernet, IPv4, UDP, IEX-TP segment with every message in it); pcapng section, interface and packet blocks written anew with zero timestamps |
| Fixture size, SHA-256 | 1 608 bytes, `8439b23c302f398c216b9b903b5994c967f4da88a22230558066faea435c56f6` |
| Generator | `julia test/fixtures/extract_iex_hist.jl <capture> AAPL <output>` |
| Decoded | 10 AAPL prints, the first ten of the day on IEX: nine before the regular session (flag `T`), nine odd lots (flag `I`); against the SIP, each matched by a print on exchange `V` equal in price and size, stamped 0.24 to 0.71 ms later |

## `itch50_AAPL_20251209.bin` (local only)

Nasdaq publishes its TotalView-ITCH sample day files at
`https://emi.nasdaq.com/ITCH/` without a licence; its only statement of use
(Nasdaq Data News #2008-91) offers them to customers for internal testing.
The excerpt is therefore not part of the repository: cut it from the
downloaded day file, and the test that reads it runs; without it the test is
skipped.

| | |
|---|---|
| Source | `S120925-v50.txt.gz`, `https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/` |
| Source size, SHA-256 | 7 929 915 419 bytes, `b6bf2f5dbd87445e37a58951c6119908ef0ea559ca373be80a0346ab73e14d66` |
| Content | 37 BinaryFILE-framed messages in day-file order, unaltered: every System Event, AAPL's Stock Directory message and its two crosses with shares, its first ten displayed executions (`E`) and five printable executions with price (`C`) with the Add Orders they execute, and its first five hidden trades (`P`) |
| Fixture size, SHA-256 | 1 263 bytes, `ce84fd491ac12938dfc237295e2f70545ad394d1187af0fcbf337d24773a7b46` |
| Generator | `julia test/fixtures/extract_itch50.jl S120925-v50.txt.gz AAPL test/fixtures/itch50_AAPL_20251209.bin` |
| Decoded | 22 AAPL prints: fifteen before the regular session, the opening cross at 09:30 and the closing cross at 16:00 New York time; against the SIP, each matched by a print on exchange `Q` equal in price and size, stamped 12.8 to 18.5 µs later, with the same odd-lot and cross flags (`I`, `O`, `6`) |
