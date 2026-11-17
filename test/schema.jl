@testset "schema / timestamps" begin
    ns = rfc3339_to_ns("2026-07-30T14:30:00.123456789Z")
    @test ns % MarketTickStreamer.NS_PER_SEC == 123_456_789
    @test ns_to_rfc3339(ns) == "2026-07-30T14:30:00.123456789Z"
    # lossless round-trip at all fractional widths
    for frac in ("1", "123", "123456", "999999999")
        s = "2020-01-02T03:04:05.$(rpad(frac, 9, '0'))Z"
        @test ns_to_rfc3339(rfc3339_to_ns(s)) == s
    end
    # numeric offsets normalize to UTC
    @test rfc3339_to_ns("2026-07-30T10:30:00-04:00") ==
          rfc3339_to_ns("2026-07-30T14:30:00Z")
    @test rfc3339_to_ns("2026-07-30T16:30:00+02:00") ==
          rfc3339_to_ns("2026-07-30T14:30:00Z")
    # no fractional part
    @test rfc3339_to_ns("2026-07-30T14:30:00Z") % MarketTickStreamer.NS_PER_SEC == 0
    @test_throws ArgumentError rfc3339_to_ns("garbage")
    @test_throws ArgumentError rfc3339_to_ns("2026-07-30 14:30")
    # 2026-07-30 20:30 UTC is 16:30 in New York (same trading day),
    # but 2026-07-31 01:00 UTC is still 2026-07-30 in New York.
    @test trading_date(rfc3339_to_ns("2026-07-30T20:30:00Z")) == Date(2026, 7, 30)
    @test trading_date(rfc3339_to_ns("2026-07-31T01:00:00Z")) == Date(2026, 7, 30)
    @test now_ns() > rfc3339_to_ns("2026-01-01T00:00:00Z")
    # The last nanosecond of an exchange day files under that day:
    # `trading_date` floors in integer arithmetic, never rounding up a date.
    @test trading_date(rfc3339_to_ns("2026-07-31T03:59:59.999999999Z")) == Date(2026, 7, 30)                      # 23:59:59.999999999 EDT
    @test trading_date(rfc3339_to_ns("2026-07-31T04:00:00Z")) == Date(2026, 7, 31)
    @test trading_date(rfc3339_to_ns("2025-11-04T04:59:59.999999999Z")) == Date(2025, 11, 3)                      # the same instant in EST
    @test trading_date(rfc3339_to_ns("2025-11-04T05:00:00Z")) == Date(2025, 11, 4)
end

@testset "backfill window follows exchange dates, not UTC days" begin
    # The request window of a date spans exactly the instants
    # `trading_date` files under it, in every offset regime.
    alpaca = AlpacaProvider("k", "s", "sip", "", "", "")
    bounds(d) = (
        MarketTickStreamer.exchange_day_start_ns(alpaca, d),
        MarketTickStreamer.exchange_day_start_ns(alpaca, d + Day(1)) - 1,
    )
    for d in (
        Date(2026, 7, 1),      # EDT, UTC-4
        Date(2025, 11, 3),     # EST, UTC-5
        Date(2025, 11, 2),     # 25-hour day, DST ends
        Date(2026, 3, 8),      # 23-hour day, DST begins
    )
        lo, hi = bounds(d)
        @test trading_date(lo) == d          # first instant of the date
        @test trading_date(hi) == d          # last instant of the date
        @test trading_date(lo - 1) == d - Day(1)
        @test trading_date(hi + 1) == d + Day(1)
    end
    # Local midnight in each offset regime.
    @test ns_to_rfc3339(bounds(Date(2026, 7, 1))[1]) == "2026-07-01T04:00:00.000000000Z"
    @test ns_to_rfc3339(bounds(Date(2025, 11, 3))[1]) == "2025-11-03T05:00:00.000000000Z"
end

@testset "schema: quote and bar frames" begin
    wire(m) = JSON.parse(JSON.json(m))
    q = MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("AAPL", 1)), 7)
    @test q.symbol == "AAPL"
    @test q.bid_price == 101.0 && q.ask_price == 101.5
    @test q.bid_size == 1.0 && q.ask_size == 2.0
    @test q.bid_exchange == "V" && q.ask_exchange == "W"
    @test q.conditions == ["R"] && q.tape == "C"
    @test q.recv_ns == 7
    @test q.time_ns == rfc3339_to_ns("2026-07-30T14:30:01.000000001Z")

    b = MarketTickStreamer.parse_alpaca_bar(wire(mock_bar("AAPL", 1)), 9)
    # `c` on a bar frame is the closing price, not a condition list.
    @test b.open == 100.0 && b.high == 101.0 && b.low == 99.0 && b.close == 100.5
    @test b.volume == 1000.0 && b.trade_count == 42 && b.vwap == 100.25
    @test b.recv_ns == 9
    # A streamed bar is one minute wide and stamped with its opening instant.
    @test b.close_ns - b.time_ns == 60 * 1_000_000_000
    later = Bar(
        b.symbol,
        b.time_ns,
        b.close_ns + 1,
        b.recv_ns,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,
        b.trade_count,
        b.vwap,
    )
    @test later != b && hash(later) != hash(b)

    # Value semantics, as for Trade: equal content compares equal.
    @test q == MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("AAPL", 1)), 7)
    @test b == MarketTickStreamer.parse_alpaca_bar(wire(mock_bar("AAPL", 1)), 9)
    @test hash(q) ==
          hash(MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("AAPL", 1)), 7))
    @test q != MarketTickStreamer.parse_alpaca_quote(wire(mock_quote("MSFT", 1)), 7)
end
