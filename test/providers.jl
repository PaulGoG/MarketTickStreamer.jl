@testset "provider registry and a second provider" begin
    # Feed names, calendar and credentials come from each provider's spec,
    # never from one vendor's vocabulary.
    @test_throws ArgumentError provider_spec("kraken")
    @test provider_spec("alpaca").tz == tz"America/New_York"
    @test provider_spec("binance").tz == tz"UTC"
    @test provider_spec("alpaca").credential_keys ==
          ["ALPACA_API_KEY_ID", "ALPACA_SECRET_KEY"]
    @test isempty(provider_spec("binance").credential_keys)
    @test MarketTickStreamer.registered_providers() == ["alpaca", "binance"]

    bp = BinanceProvider("aggTrade", "http://x", "ws://x")
    ap = AlpacaProvider("k", "s", "sip", "", "", "")
    @test always_open(bp) && !always_open(ap)
    @test exchange_tz(bp) == tz"UTC"

    # A 24-hour venue trades on weekends; an equity venue does not. The
    # default is every day, so a provider that rests must opt out — the
    # reverse default silently drops two days in seven.
    @test length(session_days(bp, Date(2026, 1, 2), Date(2026, 1, 6))) == 5
    @test session_days(ap, Date(2026, 1, 2), Date(2026, 1, 6)) ==
          [Date(2026, 1, 2), Date(2026, 1, 5), Date(2026, 1, 6)]

    # UTC has no DST, so every Binance day is exactly 24 h.
    d0 = MarketTickStreamer.exchange_day_start_ns(bp, Date(2026, 1, 15))
    d1 = MarketTickStreamer.exchange_day_start_ns(bp, Date(2026, 1, 16))
    @test d1 - d0 == 86_400 * MarketTickStreamer.NS_PER_SEC
    @test ns_to_rfc3339(d0) == "2026-01-15T00:00:00.000000000Z"
    # The New York equivalents are not 24 h apart across a DST boundary.
    n0 = MarketTickStreamer.exchange_day_start_ns(ap, Date(2025, 11, 2))
    n1 = MarketTickStreamer.exchange_day_start_ns(ap, Date(2025, 11, 3))
    @test n1 - n0 == 25 * 3600 * MarketTickStreamer.NS_PER_SEC

    @test market_clock(bp) == (; is_open = true, next_open = "", next_close = "")
    # No countable historical tape: coverage_report is refused, not a MethodError.
    @test_throws ArgumentError historical_trade_count(
        bp,
        "BTCUSDT",
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
    )

    # An unknown provider must fail at construction with a message naming
    # it, not with a MethodError deep in the session. The fallback throws
    # rather than returning, so the provider type stays narrow: a fallback
    # with a value widens every caller's inference and leaves each
    # downstream provider call with a no-matching-method branch.
    cfg_now = load_config(MarketTickStreamer.DEFAULT_CONFIG)
    @test_throws ArgumentError MarketTickStreamer.make_provider(Val(:kraken), cfg_now)
    @test Base.infer_return_type(MarketTickStreamer._provider_from_config, (Config,)) ==
          Union{AlpacaProvider,BinanceProvider}
end

@testset "provider registry: an adapter defined outside the package" begin
    # Defining provider_spec for a new name registers it: the
    # configuration accepts the name and its table, the session layer
    # constructs it, and nothing in the package is edited.
    @eval begin
        struct ToyProvider <: MarketTickStreamer.AbstractProvider
            base::String
            token::String
        end
        MarketTickStreamer.provider_spec(::Val{:toy}) = MarketTickStreamer.ProviderSpec(;
            feeds = ["ticks"],
            backfill_feeds = ["ticks"],
            tz = tz"UTC",
            credential_keys = ["TOY_TOKEN"],
            endpoint_keys = ["base"],
            max_page_limit = 100,
            regular_session = (0.0, 24.0),
            price_unit = "USD",
            size_unit = "units",
            timestamp_unit = "nanosecond",
        )
        MarketTickStreamer.make_provider(::Val{:toy}, cfg::Config) =
            ToyProvider(cfg.endpoints["base"], load_credentials!(cfg)["TOY_TOKEN"])
    end
    @test MarketTickStreamer.registered_providers() == ["alpaca", "binance", "toy"]
    mktempdir() do dir
        p = joinpath(dir, "toy.toml")
        toml =
            "[provider]\nname = \"toy\"\nfeed = \"ticks\"\n" *
            "[stream]\nsymbols = [\"X\"]\n[toy]\nbase = \"http://toy\"\n" *
            "[credentials]\nenv_file = \"absent.env\"\n"
        write(p, toml)
        cfg = load_config(p)
        @test cfg.provider == "toy" && cfg.feed == "ticks"
        @test cfg.endpoints == Dict("base" => "http://toy")
        withenv("TOY_TOKEN" => nothing) do
            @test_throws CredentialError MarketTickStreamer._provider_from_config(cfg)
        end
        withenv("TOY_TOKEN" => "t0k") do
            prov = MarketTickStreamer._provider_from_config(cfg)
            @test prov isa ToyProvider && prov.base == "http://toy" && prov.token == "t0k"
        end
        # Its table is checked against its own endpoint keys.
        write(p, replace(toml, "base = " => "bsae = "))
        @test_throws ArgumentError load_config(p)
        write(p, replace(toml, "feed = \"ticks\"" => "feed = \"trades\""))
        @test_throws ArgumentError load_config(p)
    end
    # An adapter that only streams declares no historical feed. Its
    # configuration loads, with nothing to name under backfill.feed; one whose
    # daily files carry another feed than its REST endpoint gets that feed as
    # the default of a bulk backfill.
    @eval begin
        MarketTickStreamer.provider_spec(::Val{:streamonly}) =
            MarketTickStreamer.ProviderSpec(;
                feeds = ["ticks"],
                backfill_feeds = String[],
                tz = tz"UTC",
                max_page_limit = 100,
                regular_session = (0.0, 24.0),
                price_unit = "USD",
                size_unit = "units",
                timestamp_unit = "nanosecond",
            )
        MarketTickStreamer.provider_spec(::Val{:dumps}) = MarketTickStreamer.ProviderSpec(;
            feeds = ["ticks"],
            backfill_feeds = ["agg"],
            bulk_feeds = ["raw"],
            tz = tz"UTC",
            max_page_limit = 100,
            regular_session = (0.0, 24.0),
            price_unit = "USD",
            size_unit = "units",
            timestamp_unit = "nanosecond",
        )
    end
    mktempdir() do dir
        p = joinpath(dir, "c.toml")
        base(name) = "[provider]\nname = \"$name\"\n[stream]\nsymbols = [\"X\"]\n"
        write(p, base("streamonly"))
        @test Base.invokelatest(load_config, p).backfill_feed == ""
        write(p, base("streamonly") * "[backfill]\nfeed = \"ticks\"\n")
        @test_throws ArgumentError Base.invokelatest(load_config, p)
        write(p, base("dumps"))
        @test Base.invokelatest(load_config, p).backfill_feed == "agg"
        write(p, base("dumps") * "[backfill]\nsource = \"bulk\"\n")
        @test Base.invokelatest(load_config, p).backfill_feed == "raw"
        write(p, base("dumps") * "[backfill]\nsource = \"bulk\"\nfeed = \"agg\"\n")
        @test_throws ArgumentError Base.invokelatest(load_config, p)
    end
    Base.delete_method(which(MarketTickStreamer.provider_spec, (Val{:streamonly},)))
    Base.delete_method(which(MarketTickStreamer.provider_spec, (Val{:dumps},)))
    # The registry is global state: take the adapter out again.
    Base.delete_method(which(MarketTickStreamer.provider_spec, (Val{:toy},)))
    Base.delete_method(which(MarketTickStreamer.make_provider, (Val{:toy}, Config)))
    @test Base.invokelatest(MarketTickStreamer.registered_providers) ==
          ["alpaca", "binance"]
end

@testset "binance: frame and REST parsing" begin
    f = mock_binance_frame("BTCUSDT", 4)
    t = MarketTickStreamer.parse_binance_trade(JSON.parse(JSON.json(f)), 99)
    @test t.symbol == "BTCUSDT"
    @test t.recv_ns == 99
    @test t.exchange == "BINANCE" && t.tape == "SPOT"
    @test t.id == 4
    # Prices and sizes arrive as strings and must survive as Float64.
    @test isapprox(t.price, 64_004.0; rtol = 1e-12) && isapprox(t.size, 0.004; rtol = 1e-12)
    # Milliseconds on the wire by default, nanoseconds in the schema.
    @test t.time_ns == Int64(f.T) * 1_000_000
    @test t.time_ns % 1_000_000 == 0
    # The adapter asks for microseconds; a frame in either unit lands on
    # the same instant, and the stream URL carries the request.
    fu = mock_binance_frame("BTCUSDT", 4; unit = :us)
    @test fu.T == f.T * 1000
    @test MarketTickStreamer.parse_binance_trade(JSON.parse(JSON.json(fu)), 0).time_ns ==
          t.time_ns
    @test MarketTickStreamer._binance_ns(1_791_590_429_424) == 1_791_590_429_424_000_000
    @test MarketTickStreamer._binance_ns(1_791_590_429_424_532) == 1_791_590_429_424_532_000
    p0 = BinanceProvider("trade", "http://x", "ws://y")
    url = MarketTickStreamer.ws_url(p0, ["BTCUSDT", "ETHUSDT"])
    @test occursin("streams=btcusdt@trade/ethusdt@trade", url)
    @test endswith(url, "&timeUnit=MICROSECOND")
    # `m = true` means the buyer was the maker, so the seller crossed.
    @test t.conditions == (f.m ? ["sell"] : ["buy"])
    # k = 6 is even, so the mock sets m = false: the buyer crossed.
    @test MarketTickStreamer.parse_binance_trade(
        JSON.parse(JSON.json(mock_binance_frame("BTCUSDT", 6))),
        0,
    ).conditions == ["buy"]
    # No tape entry means nothing to exclude on: every crypto print is
    # price-forming, there being no odd lots or late prints.
    @test price_forming(t)
    # A raw `trade` frame carries `t` for the id where aggTrade carries `a`.
    raw = (;
        e = "trade",
        E = f.T,
        s = "BTCUSDT",
        t = 77,
        p = "1.5",
        q = "2.0",
        T = f.T,
        m = false,
        M = true,
    )
    @test MarketTickStreamer.parse_binance_trade(JSON.parse(JSON.json(raw)), 0).id == 77
end

@testset "binance: id-seeded backfill pagination" begin
    rest_port = freeport(9811)
    per_day = 7
    units = String[]
    server =
        start_mock_binance_rest(rest_port; trades_per_day = per_day, seen_units = units)
    try
        p = BinanceProvider("aggTrade", "http://127.0.0.1:$rest_port", "ws://unused")
        # One UTC date, walked in pages of 3: the seed request uses
        # startTime, every later one fromId.
        got = historical_trades(
            p,
            "BTCUSDT",
            MOCK_BINANCE_EPOCH,
            MOCK_BINANCE_EPOCH;
            page_limit = 3,
            rate_sleep_s = 0.0,
        ).trades
        @test length(got) == per_day
        @test all(t -> trading_date(t.time_ns; tz = tz"UTC") == MOCK_BINANCE_EPOCH, got)
        @test issorted([t.time_ns for t in got])
        @test [t.id for t in got] == collect(1:per_day)
        @test all(t -> t.recv_ns == 0, got)        # never crossed the wire
        # Every request asked for microseconds, the mock answered in them,
        # and the instants are the tape's.
        @test !isempty(units) && all(==("MICROSECOND"), units)
        @test all(t -> t.time_ns == mock_binance_row(t.id, per_day).T * 1_000_000, got)

        # Two dates must give exactly two days of tape, weekend included.
        two = historical_trades(
            p,
            "BTCUSDT",
            MOCK_BINANCE_EPOCH,
            MOCK_BINANCE_EPOCH + Day(1);
            page_limit = 3,
            rate_sleep_s = 0.0,
        ).trades
        @test length(two) == 2 * per_day
        @test length(unique(trading_date(t.time_ns; tz = tz"UTC") for t in two)) == 2

        # Streaming mode counts the prints and retains none.
        pages = Vector{Int}()
        r = historical_trades(
            p,
            "BTCUSDT",
            MOCK_BINANCE_EPOCH,
            MOCK_BINANCE_EPOCH;
            page_limit = 3,
            rate_sleep_s = 0.0,
            each_page = pg -> push!(pages, length(pg)),
        )
        @test r.prints == per_day && isempty(r.trades)
        @test sum(pages) == per_day && length(pages) > 1

        # Only aggTrade can seek by time; asking for the raw feed must say so.
        @test_throws ArgumentError historical_trades(
            p,
            "BTCUSDT",
            MOCK_BINANCE_EPOCH,
            MOCK_BINANCE_EPOCH;
            feed = "trade",
        )
    finally
        close(server)
    end
end

@testset "binance: end-to-end backfill on the UTC calendar" begin
    mktempdir() do dir
        rest_port = freeport(9831)
        per_day = 7
        server = start_mock_binance_rest(rest_port; trades_per_day = per_day)
        try
            cfg = load_config(
                mock_binance_config_toml(
                    dir;
                    ws_port = 9999,
                    rest_port = rest_port,
                    start_date = string(MOCK_BINANCE_EPOCH),
                    end_date = string(MOCK_BINANCE_EPOCH + Day(2)),
                ),
            )
            @test cfg.provider == "binance"
            @test cfg.exchange_tz == tz"UTC"
            files = run_backfill(cfg)
            # Three consecutive dates, one file each, the weekend included;
            # an equity day loop would have skipped it.
            @test length(files) == 3
            days = sort([basename(f)[1:10] for f in files])
            @test days == [string(MOCK_BINANCE_EPOCH + Day(i)) for i in 0:2]
            rows = CSV.read(first(sort(files)), DataFrame)
            @test nrow(rows) == per_day
            @test all(rows.tape .== "SPOT") && all(rows.exchange .== "BINANCE")
            meta = TOML.parsefile(
                only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
            )
            @test meta["provenance"]["timestamp_unit"] == "microsecond"
        finally
            close(server)
        end
    end
end

@testset "binance: bulk dump parsing" begin
    d = Date(2026, 1, 1)
    # The reader's page state: `kept` holds the trades, `prints` counts them.
    parse_pages(text; feed = "aggTrade", kw...) = MarketTickStreamer._parse_bulk_csv(
        Vector{UInt8}(codeunits(text)),
        "BTCUSDT",
        d,
        feed;
        name = "x.csv",
        kw...,
    )
    parse_csv(text; kw...) = parse_pages(text; kw...).kept
    us = 1_767_225_600_000_000 + 5_000_000           # 2026-01-01T00:00:05Z, µs
    ms = 1_767_225_600_000 + 5_000                   # the same instant, ms
    agg = "7,64000.5,0.25,10,12,$us,True,True\n8,64001.0,0.5,13,13,$(us + 1_000),False,True\n"
    got = parse_csv(agg)
    @test length(got) == 2
    @test got[1] == Trade(
        "BTCUSDT",
        1_767_225_605_000_000_000,
        0,
        64000.5,
        0.25,
        "BINANCE",
        ["sell"],
        "SPOT",
        7,
    )
    @test got[2].conditions == ["buy"]
    @test got[2].time_ns == 1_767_225_605_001_000_000 && got[2].id == 8
    # Millisecond timestamps, the unit before 2025-01-01, read to the same instant.
    @test parse_csv("7,64000.5,0.25,10,12,$ms,true,True\n")[1].time_ns == got[1].time_ns
    # A header line is skipped; the trades feed has seven columns.
    header = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker,is_best_match\n"
    @test length(parse_csv(header * agg)) == 2
    tr = parse_csv("10,64000.5,0.145,9280.07,$us,False,True\n"; feed = "trade")
    @test tr[1].id == 10 && tr[1].size == 0.145 && tr[1].conditions == ["buy"]
    # Paging: the count comes back and every page reaches the callback.
    pages = Vector{Int}()
    r = parse_pages(agg^3; each_page = pg -> push!(pages, length(pg)), page_rows = 4)
    @test r.prints == 6 && isempty(r.kept) && pages == [4, 2]
    # A wrong column count and a row outside the day are errors, not silence.
    # Trade ids: each row should follow the previous one by one.
    row(id) = "$id,64000.5,0.25,$id,$id,$us,True,True\n"
    @test parse_pages(row(7) * row(8) * row(9)).id_breaks == 0
    @test parse_pages(row(7) * row(9)).id_breaks == 1               # id 8 missing
    @test parse_pages(row(7) * row(8) * row(7) * row(8)).id_breaks == 1   # repeated run
    @test parse_pages(row(7) * row(8); previous_id = 6).id_breaks == 0
    @test parse_pages(row(7) * row(8); previous_id = 5).id_breaks == 1    # day boundary
    @test parse_pages(row(7) * row(8)).last_id == 8
    @test parse_pages("").last_id === nothing
    @test parse_pages(""; previous_id = 5).last_id == 5     # no row: the reference passes on
    @test_throws IntegrityError parse_csv("7,64000.5,0.25,$us,True\n")
    @test_throws IntegrityError parse_csv(
        "7,64000.5,0.25,10,12,$(us + 86_400_000_000),True,True\n",
    )
end

@testset "binance: bulk files match the REST tape, are verified and kept" begin
    mktempdir() do dir
        rest_port = freeport(9871)
        per_day = 7
        server = start_mock_binance_rest(
            rest_port;
            trades_per_day = per_day,
            missing_dates = [MOCK_BINANCE_EPOCH + Day(2)],
        )
        base = "http://127.0.0.1:$rest_port"
        dest = joinpath(dir, "bulk")
        try
            p = BinanceProvider("aggTrade", base, "ws://unused", base)
            @test MarketTickStreamer.bulk_url(p, "btcusdt", MOCK_BINANCE_EPOCH) ==
                  "$base/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-01-01.zip"
            @test_throws ArgumentError MarketTickStreamer.bulk_url(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH;
                feed = "kline",
            )
            b = bulk_trades(p, "BTCUSDT", MOCK_BINANCE_EPOCH; dest_dir = dest)
            rest = historical_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH,
                MOCK_BINANCE_EPOCH;
                page_limit = 3,
                rate_sleep_s = 0.0,
            ).trades
            @test b.trades == rest && b.prints == length(rest)
            zip = joinpath(dest, "binance", "BTCUSDT", "BTCUSDT-aggTrades-2026-01-01.zip")
            @test b.file == zip
            @test isfile(zip) && isfile(zip * ".CHECKSUM") && !isfile(zip * ".partial")
            # An interrupted download resumes from its partial file with a
            # Range request and still passes the checksum.
            full = read(zip)
            rm(zip)
            write(zip * ".partial", full[1:(length(full)÷2)])
            @test bulk_trades(p, "BTCUSDT", MOCK_BINANCE_EPOCH; dest_dir = dest).trades ==
                  rest
            @test read(zip) == full && !isfile(zip * ".partial")
            # The trades feed is the same tape under its own column layout.
            tr = bulk_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH + Day(1);
                feed = "trade",
                dest_dir = dest,
            ).trades
            @test length(tr) == per_day
            @test [t.id for t in tr] == collect((per_day+1):2per_day)
            @test isfile(
                joinpath(dest, "binance", "BTCUSDT", "BTCUSDT-trades-2026-01-02.zip"),
            )
            # Streaming counts the prints and keeps none; an unpublished
            # day is empty, names no file and leaves none behind.
            pages = Vector{Int}()
            r = bulk_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH;
                dest_dir = dest,
                each_page = pg -> push!(pages, length(pg)),
                page_rows = 3,
            )
            @test r.prints == per_day && isempty(r.trades) && pages == [3, 3, 1]
            gone = MOCK_BINANCE_EPOCH + Day(2)
            @test bulk_trades(p, "BTCUSDT", gone; dest_dir = dest) == (;
                trades = Trade[],
                prints = 0,
                file = "",
                id_breaks = 0,
                last_id = nothing,
            )
            @test bulk_trades(p, "BTCUSDT", gone; dest_dir = dest, each_page = identity).prints ==
                  0
            @test !isfile(
                joinpath(dest, "binance", "BTCUSDT", "BTCUSDT-aggTrades-2026-01-03.zip"),
            )
        finally
            close(server)
        end
        # A verified file is reused with the venue unreachable.
        p = BinanceProvider("aggTrade", base, "ws://unused", base, RestPolicy(1, 1, 0))
        @test bulk_trades(p, "BTCUSDT", MOCK_BINANCE_EPOCH; dest_dir = dest).prints ==
              per_day
        # A checksum that does not match discards the download and fails the day.
        rest_port = freeport(9881)
        server = start_mock_binance_rest(
            rest_port;
            trades_per_day = per_day,
            corrupt_checksum = true,
        )
        try
            base = "http://127.0.0.1:$rest_port"
            p = BinanceProvider("aggTrade", base, "ws://unused", base)
            bad = joinpath(dir, "bad")
            @test_throws IntegrityError bulk_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH;
                dest_dir = bad,
            )
            @test !isfile(
                joinpath(bad, "binance", "BTCUSDT", "BTCUSDT-aggTrades-2026-01-01.zip"),
            )
        finally
            close(server)
        end
        # A zip whose entry fails its own CRC-32, under a checksum that
        # matches: rejected before any row is handed over.
        rest_port = freeport(9886)
        server = start_mock_binance_rest(
            rest_port;
            trades_per_day = per_day,
            corrupt_entry = true,
        )
        try
            base = "http://127.0.0.1:$rest_port"
            p = BinanceProvider("aggTrade", base, "ws://unused", base)
            pages = Vector{Int}()
            @test_throws IntegrityError bulk_trades(
                p,
                "BTCUSDT",
                MOCK_BINANCE_EPOCH;
                dest_dir = joinpath(dir, "crc"),
                each_page = pg -> push!(pages, length(pg)),
            )
            @test isempty(pages)
        finally
            close(server)
        end
    end
end

@testset "binance: end-to-end bulk backfill, then resume" begin
    mktempdir() do dir
        rest_port = freeport(9891)
        per_day = 7
        # The second day's file repeats its first three rows, as a real
        # vendor file did; compaction drops them and the sidecar says so.
        repeated = MOCK_BINANCE_EPOCH + Day(1)
        server = start_mock_binance_rest(
            rest_port;
            trades_per_day = per_day,
            repeat_head = Dict(repeated => 3),
        )
        try
            cfg = load_config(
                mock_binance_config_toml(
                    dir;
                    ws_port = 9999,
                    rest_port = rest_port,
                    source = "bulk",
                    backfill_feed = "trade",
                    start_date = string(MOCK_BINANCE_EPOCH),
                    end_date = string(MOCK_BINANCE_EPOCH + Day(2)),
                ),
            )
            @test cfg.backfill_source === :bulk && cfg.backfill_feed == "trade"
            files = run_backfill(cfg)
            @test length(files) == 3
            rows = CSV.read(first(sort(files)), DataFrame)
            @test nrow(rows) == per_day
            @test rows.id == collect(1:per_day) && all(rows.recv_ns .== 0)
            @test all(rows.tape .== "SPOT") && all(rows.exchange .== "BINANCE")
            @test all(
                isfile(
                    joinpath(
                        cfg.bulk_dir,
                        "binance",
                        "BTCUSDT",
                        "BTCUSDT-trades-$(MOCK_BINANCE_EPOCH + Day(i)).zip",
                    ),
                ) for i in 0:2
            )
            raw = filter(f -> endswith(f, ".jsonl"), readdir(cfg.raw_dir))
            @test !isempty(raw) && all(startswith("backfill_bulk_"), raw)
            rep = CSV.read(
                joinpath(cfg.processed_dir, "BTCUSDT", "$(repeated).csv"),
                DataFrame,
            )
            @test nrow(rep) == per_day && allunique(rep.id)
            meta = TOML.parsefile(
                only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
            )
            @test meta["session"]["ticks"] == 3 * per_day + 3        # received
            @test meta["session"]["ticks_processed"] == 3 * per_day  # stored
            # The mock's ids run on across days; the repeated head of day two
            # is the one break.
            @test meta["session"]["id_breaks"] == 1
            # Resume: every day is under processed/, nothing is redone.
            @test isempty(run_backfill(cfg))
        finally
            close(server)
        end
    end
end

@testset "binance: trade-id breaks across the days of a bulk backfill" begin
    per_day = 7
    day(i) = MOCK_BINANCE_EPOCH + Day(i)
    # (port, files left short, unpublished days, breaks, prints). Day two
    # lacking its first row shows only against the last id of day one, so
    # the count needs the link across days; an empty or unpublished day two
    # passes that id on, and day three's first row is measured against it.
    cases = [
        (9931, Dict(day(1) => 1), Date[], 1, 3 * per_day - 1),
        (9936, Dict(day(1) => per_day, day(2) => 1), Date[], 1, 2 * per_day - 1),
        (9941, Dict{Date,Int}(), [day(1)], 1, 2 * per_day),
    ]
    for (port, drop_head, missing_dates, breaks, prints) in cases
        mktempdir() do dir
            rest_port = freeport(port)
            server = start_mock_binance_rest(
                rest_port;
                trades_per_day = per_day,
                drop_head,
                missing_dates,
            )
            try
                cfg = load_config(
                    mock_binance_config_toml(
                        dir;
                        ws_port = 9999,
                        rest_port,
                        source = "bulk",
                        backfill_feed = "trade",
                        start_date = string(day(0)),
                        end_date = string(day(2)),
                    ),
                )
                run_backfill(cfg)
                meta = TOML.parsefile(
                    only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
                )
                @test meta["session"]["id_breaks"] == breaks
                @test meta["session"]["ticks"] == prints
            finally
                close(server)
            end
        end
    end
end

@testset "binance: bulk backfill without the raw copy" begin
    mktempdir() do dir
        rest_port = freeport(9911)
        per_day = 7
        days = [MOCK_BINANCE_EPOCH + Day(i) for i in 0:1]
        toml(; resume) = mock_binance_config_toml(
            dir;
            ws_port = 9999,
            rest_port,
            source = "bulk",
            backfill_feed = "trade",
            start_date = string(first(days)),
            end_date = string(last(days)),
            keep_raw = false,
            resume,
        )
        server = start_mock_binance_rest(rest_port; trades_per_day = per_day)
        cfg = load_config(toml(; resume = true))
        local files
        try
            files = run_backfill(cfg)
        finally
            close(server)
        end
        @test length(files) == 2
        @test all(f -> nrow(CSV.read(f, DataFrame)) == per_day, files)
        # Only the sidecar and its manifest remain in raw/; the vendor
        # files stay as the record of each day.
        @test isempty(filter(endswith(".jsonl"), readdir(cfg.raw_dir)))
        meta = TOML.parsefile(
            only(filter(endswith(".meta.toml"), readdir(cfg.raw_dir; join = true))),
        )
        @test meta["session"]["ticks_processed"] == 2 * per_day
        @test isempty(meta["session"]["raw_files"])
        @test all(
            isfile(joinpath(cfg.bulk_dir, "binance", "BTCUSDT", "BTCUSDT-trades-$d.zip"))
            for d in days
        )
        # With the server gone, a rerun that does not resume rebuilds every
        # day from the kept files, byte for byte, so no backup is made.
        before = Dict(f => read(f) for f in files)
        rebuilt = run_backfill(load_config(toml(; resume = false)))
        @test sort(rebuilt) == sort(files)
        backup(f) = string(first(splitext(f)), "_#1", last(splitext(f)))
        @test all(f -> !isfile(backup(f)) && read(f) == before[f], rebuilt)
    end
end

@testset "binance: a quiet stream does not use up the reconnects" begin
    mktempdir() do dir
        ws_port = freeport(9856)
        # A Binance stream sends nothing until a trade: no frame at all.
        # Each connection stays up until the watchdog cuts it, so none is
        # a failed attempt and the session runs to its deadline.
        server = start_mock_binance_ws(
            ws_port;
            frames = Any[],
            linger_s = 30.0,
            fatal_after = 1_000,
        )
        try
            cfg = load_config(
                mock_binance_config_toml(
                    dir;
                    ws_port,
                    rest_port = 9999,
                    stale_timeout_s = 1.0,
                    # 14.4 s: the first connections of a cold process
                    # take seconds to compile
                    max_session_hours = 0.004,
                ),
            )
            s = live_source(BinanceProvider(cfg), cfg)
            @test isempty(collect(s.channel))
            @test s.outcome[] === :completed
            @test s.stats[].reconnects >= 2
        finally
            close(server)
        end
    end
end

@testset "binance: live stream needs no auth handshake" begin
    mktempdir() do dir
        ws_port = freeport(9851)
        frames = [mock_binance_frame("BTCUSDT", k) for k in 1:5]
        server = start_mock_binance_ws(ws_port; frames = frames)
        try
            cfg = load_config(
                mock_binance_config_toml(
                    dir;
                    ws_port = ws_port,
                    rest_port = 9999,
                    symbols = ["BTCUSDT"],
                ),
            )
            p = BinanceProvider(cfg)
            s = live_source(p, cfg)
            got = collect(s.channel)
            @test length(got) == 5
            @test all(t -> t.symbol == "BTCUSDT", got)
            @test all(t -> t.recv_ns > 0, got)     # live prints are stamped
            @test [t.id for t in got] == collect(1:5)
        finally
            close(server)
        end
    end
end

@testset "binance: a stream error frame is fatal, no retry storm" begin
    mktempdir() do dir
        ws_port = freeport(9852)
        frames = [mock_binance_frame("BTCUSDT", k) for k in 1:5]
        server = start_mock_binance_ws(ws_port; frames = frames)
        try
            cfg = load_config(
                mock_binance_config_toml(
                    dir;
                    ws_port = ws_port,
                    rest_port = 9999,
                    symbols = ["BTCUSDT"],
                    reconnect_max_retries = 3,
                ),
            )
            s = live_source(BinanceProvider(cfg), cfg)
            got = collect(s.channel)
            @test length(got) == 5
            @test s.stop[]    # the producer's end stops the guard tasks
            # The first connection closes after its frames (retryable); the
            # second answers with the error frame: one reconnect, then abort.
            @test s.stats[].reconnects == 1
            @test s.outcome[] === :failed
        finally
            close(server)
        end
    end
end
