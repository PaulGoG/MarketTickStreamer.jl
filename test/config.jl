@testset "config" begin
    # The shipped configuration must always be valid. Its path is given:
    # an installed copy refuses the no-argument form.
    cfg = load_config(MarketTickStreamer.DEFAULT_CONFIG)
    @test cfg.provider == "alpaca"
    @test !isempty(cfg.symbols)
    @test cfg.feed in ("iex", "delayed_sip", "sip")
    mktempdir() do dir
        bad = joinpath(dir, "bad.toml")
        write(bad, "[provider]\nfeed = \"nope\"\n[stream]\nsymbols = [\"A\"]\n")
        @test_throws ArgumentError load_config(bad)
        write(bad, "[stream]\nsymbols = []\n")
        @test_throws ArgumentError load_config(bad)
        write(bad, "[stream]\nsymbols = [\"A\"]\nchannels = [\"trades\", \"news\"]\n")
        @test_throws ArgumentError load_config(bad)
        @test_throws ArgumentError load_config(joinpath(dir, "missing.toml"))
    end
    mktempdir() do dir
        base = "[stream]\nsymbols = [\"A\"]\n"
        exchange_today = Date(now(tz"America/New_York"))
        p = joinpath(dir, "dates.toml")
        write(p, base * "[backfill]\nstart_date = \"today-2d\"\nend_date = \"today\"\n")
        c = load_config(p)
        @test c.backfill_start == exchange_today - Day(2)
        @test c.backfill_end == exchange_today
        write(p, base * "[backfill]\nstart_date = \"today\"\nend_date = \"today\"\n")
        @test load_config(p).backfill_start == exchange_today
        write(
            p,
            base * "[backfill]\nstart_date = \"2026-08-12\"\nend_date = \"2026-08-13\"\n",
        )
        c = load_config(p)
        @test c.backfill_start == Date(2026, 8, 12)
        @test c.backfill_end == Date(2026, 8, 13)
        write(p, base * "[backfill]\nstart_date = \"yesterday\"\n")
        @test_throws ArgumentError load_config(p)
        write(p, base * "[backfill]\nstart_date = \"today+2d\"\n")
        @test_throws ArgumentError load_config(p)
        write(p, base * "[backfill]\nstart_date = \"today\"\nend_date = \"today-1d\"\n")
        @test_throws ArgumentError load_config(p)
    end
    mktempdir() do dir
        base = "[stream]\nsymbols = [\"A\"]\n"
        binance = "[provider]\nname = \"binance\"\n"
        p = joinpath(dir, "bounds.toml")
        rejected(body) = (write(p, body); @test_throws ArgumentError load_config(p))
        # a misspelled table or key must not fall back to a default
        rejected("[strem]\nsymbols = [\"A\"]\n")
        rejected(base * "stale_timout_s = 5.0\n")
        # types
        rejected(base * "stale_timeout_s = \"5\"\n")
        rejected(base * "require_market_open = 1\n")
        rejected(base * "[limits]\nchannel_capacity = 2.5\n")
        rejected("[stream]\nsymbols = \"A\"\n")
        rejected(base * "[alpaca]\ndata_base = 8080\n")
        rejected(base * "[alpaca]\nrest_base = \"http://x\"\n")   # a Binance key
        rejected(base * "[kraken]\nrest_base = \"http://x\"\n")   # no such provider
        # bounds
        rejected(base * "stale_timeout_s = 0.0\n")
        rejected(base * "stale_timeout_s = inf\n")              # no timer can be armed
        rejected(base * "stale_timeout_s = 86401.0\n")
        rejected(base * "reconnect_max_retries = -1\n")
        rejected(base * "reconnect_base_delay_s = 2.0\nreconnect_max_delay_s = 1.0\n")
        rejected(base * "[storage]\nflush_max_ticks = 0\n")
        rejected(base * "[limits]\nmax_session_hours = 0.0\n")
        # Accepted with a warning: settings that load but cannot be meant.
        warned(body, msg) = (write(p, body); @test_logs (:warn, msg) load_config(p))
        warned(base * "channels = []\n", r"subscribes to nothing")
        warned(base * "require_market_open = false\nwait_for_open = true\n", r"no effect")
        warned(
            base * "[storage]\nraw_subdir = \"x\"\nprocessed_subdir = \"x\"\n",
            r"not distinct",
        )
        # the deadline is Int64 ns
        rejected(base * "[limits]\nmax_session_hours = inf\n")
        rejected(base * "[limits]\nmax_session_hours = 3.0e6\n")
        rejected(base * "[limits]\nmax_session_hours = 2.2e6\n")   # past Int64 ns from now
        # a table, not an array
        rejected(base * "[quality]\nnon_price_conditions = [\"I\"]\n")
        rejected(base * "[backfill]\nrate_limit_sleep_s = -0.1\n")
        rejected(base * "[replay]\nclock = \"wall\"\n")
        rejected(base * "[replay]\nmax_gap_s = 0.0\n")
        rejected(base * "[replay]\nmax_gap_s = nan\n")
        rejected(base * "[limits]\nread_mem_fraction = 0.0\n")
        rejected(base * "[limits]\nread_mem_fraction = 1.5\n")
        # the page limit is the provider's: Binance clamps an over-limit
        # request without an error, which would truncate every day
        rejected(base * "[backfill]\npage_limit = 10001\n")
        rejected(binance * base * "[backfill]\npage_limit = 1001\n")
        # The bulk source is a provider capability: Alpaca has none,
        # Binance has it for both feeds, REST for aggTrade only.
        rejected(base * "[backfill]\nsource = \"ftp\"\n")
        rejected(base * "[backfill]\nsource = \"bulk\"\n")
        rejected(binance * base * "[backfill]\nsource = \"rest\"\nfeed = \"trade\"\n")
        write(p, binance * base * "[backfill]\nsource = \"bulk\"\nfeed = \"trade\"\n")
        c = load_config(p)
        @test c.backfill_source === :bulk && c.backfill_feed == "trade"
        @test endswith(c.bulk_dir, joinpath("data", "bulk"))
        @test c.backfill_keep_raw
        # Dropping the raw copy needs the vendor file as the record.
        rejected(binance * base * "[backfill]\nkeep_raw = false\n")
        rejected(binance * base * "[backfill]\nkeep_raw = \"no\"\n")
        write(p, binance * base * "[backfill]\nsource = \"bulk\"\nkeep_raw = false\n")
        @test !load_config(p).backfill_keep_raw
        write(p, base)
        @test load_config(p).backfill_source === :rest
        write(p, binance * base)
        @test load_config(p).backfill_page_limit == 1000
        write(p, base)
        c = load_config(p)
        @test c.backfill_page_limit == 10_000
        @test c.replay_clock === :auto
        @test c.replay_max_gap_s == Inf
        @test c.read_mem_fraction == 0.5
        write(p, base * "[replay]\nmax_gap_s = 3600\n")
        @test load_config(p).replay_max_gap_s == 3600.0
        # a TOML date is accepted as well as a date string
        write(p, base * "[backfill]\nstart_date = 2026-08-12\nend_date = 2026-08-12\n")
        @test load_config(p).backfill_start == Date(2026, 8, 12)
    end
    mktempdir() do dir
        p = joinpath(dir, "rng.toml")
        base = "[stream]\nsymbols = [\"A\"]\n"
        write(p, base * "[rng]\nseed = 42\n")
        @test load_config(p).rng_seed == 42
        write(p, base)
        a, b = load_config(p).rng_seed, load_config(p).rng_seed
        @test a >= 0 && b >= 0 && a != b      # drawn from the OS at each load
        write(p, base * "[rng]\nseed = -1\n")
        @test_throws ArgumentError load_config(p)
        write(p, base * "[rng]\nseed = \"7\"\n")
        @test_throws ArgumentError load_config(p)
    end
end

@testset "config: paths follow the file, new tables, credentials file" begin
    mktempdir() do dir
        base = "[stream]\nsymbols = [\"A\"]\n"
        p = joinpath(dir, "conf", "my.toml")
        mkpath(dirname(p))
        # Relative paths are relative to the configuration file.
        write(
            p,
            base *
            "[storage]\ndata_dir = \"../d\"\n[logging]\nlog_dir = \"l\"\n" *
            "[credentials]\nenv_file = \"secrets/keys.env\"\n",
        )
        cfg = load_config(p)
        @test cfg.data_dir == joinpath(dir, "d")
        @test cfg.raw_dir == joinpath(dir, "d", "raw")
        @test cfg.log_dir == joinpath(dir, "conf", "l")
        @test cfg.env_file == joinpath(dir, "conf", "secrets", "keys.env")
        # Defaults sit beside the file too, and absolute paths are kept.
        write(p, base)
        cfg = load_config(p)
        @test cfg.data_dir == joinpath(dir, "conf", "data")
        @test cfg.env_file == joinpath(dir, "conf", ".env")
        write(p, base * "[storage]\ndata_dir = \"$(joinpath(dir, "abs"))\"\n")
        @test load_config(p).data_dir == joinpath(dir, "abs")
        # The same file read through a relative path resolves identically.
        cd(dirname(p)) do
            @test load_config("my.toml").data_dir == joinpath(dir, "abs")
        end

        # New keys: defaults, then every documented bound.
        write(p, base)
        cfg = load_config(p)
        @test (
            cfg.rest_request_timeout_s,
            cfg.rest_connect_timeout_s,
            cfg.rest_max_retries,
            cfg.rest_max_download_mb_s,
            cfg.rest_download_connections,
        ) == (30, 10, 5, 0.0, 1)
        @test MarketTickStreamer.RestPolicy(cfg).max_download_rate == Inf
        write(p, base * "[rest]\nmax_download_mb_s = 5\ndownload_connections = 4\n")
        @test MarketTickStreamer.RestPolicy(load_config(p)).max_download_rate == 5e6
        @test MarketTickStreamer.RestPolicy(load_config(p)).download_connections == 4
        @test cfg.max_open_spill_files == 256
        @test cfg.compact_mem_fraction == 0.5
        @test cfg.spill_headroom == 1.1
        @test cfg.compact_footprint_factor == 4.0
        @test cfg.processed_compression === :none
        write(
            p,
            base *
            "[storage]\nprocessed_format = \"arrow\"\nprocessed_compression = \"zstd\"\n",
        )
        @test load_config(p).processed_compression === :zstd
        for bad in (
            "[storage]\nprocessed_compression = \"zstd\"\n",          # csv cannot honor it
            "[storage]\nprocessed_format = \"arrow\"\nprocessed_compression = \"gzip\"\n",
            "[rest]\nrequest_timeout_s = 0\n",
            "[rest]\nrequest_timeout_s = 1.5\n",
            "[rest]\nconnect_timeout_s = 0\n",
            "[rest]\nmax_retries = -1\n",
            "[rest]\nretries = 3\n",
            "[rest]\nmax_download_mb_s = -1.0\n",
            "[rest]\nmax_download_mb_s = inf\n",
            "[rest]\nmax_download_mb_s = \"5\"\n",
            "[rest]\ndownload_connections = 0\n",
            "[rest]\ndownload_connections = 9\n",
            "[rest]\ndownload_connections = 2.0\n",
            "[limits]\nmax_open_spill_files = 0\n",
            "[limits]\ncompact_mem_fraction = 0.0\n",
            "[limits]\ncompact_mem_fraction = 1.5\n",
            "[limits]\nspill_headroom = 0.9\n",
            "[limits]\ncompact_footprint_factor = 0\n",
            "[credentials]\nenv_file = \"\"\n",
            "[credentials]\nenv_file = 3\n",
        )
            write(p, base * bad)
            @test_throws ArgumentError load_config(p)
        end

        # Credentials come from the file the configuration names.
        write(p, base * "[credentials]\nenv_file = \"keys.env\"\n")
        write(
            joinpath(dir, "conf", "keys.env"),
            "ALPACA_API_KEY_ID=key-from-file\nALPACA_SECRET_KEY=secret-from-file\n",
        )
        chmod(joinpath(dir, "conf", "keys.env"), 0o600)
        withenv("ALPACA_API_KEY_ID" => nothing, "ALPACA_SECRET_KEY" => nothing) do
            @test load_credentials!(load_config(p)) == Dict(
                "ALPACA_API_KEY_ID" => "key-from-file",
                "ALPACA_SECRET_KEY" => "secret-from-file",
            )
        end
        # What is missing is named, as a field and in the message.
        rm(joinpath(dir, "conf", "keys.env"))
        withenv("ALPACA_API_KEY_ID" => "k", "ALPACA_SECRET_KEY" => nothing) do
            err = try
                load_credentials!(load_config(p))
            catch e
                e
            end
            @test err isa CredentialError && err.names == ["ALPACA_SECRET_KEY"]
            @test occursin("ALPACA_SECRET_KEY", sprint(showerror, err))
        end
        # A public provider asks for nothing and reads no file.
        @test load_credentials!(String[]; env_path = "/nonexistent") ==
              Dict{String,String}()
    end
    # The shipped configuration is the working one in a clone, where its
    # relative paths land in the repository, and a template elsewhere.
    root = pkgdir(MarketTickStreamer)
    @test MarketTickStreamer.DEFAULT_CONFIG == joinpath(root, "config", "config.toml")
    # A clone is never taken for an installed copy, whatever its path.
    ispath(joinpath(root, ".git")) && @test !MarketTickStreamer._installed_in_depot()
    if MarketTickStreamer._installed_in_depot()
        @test_throws ArgumentError load_config()
    else
        shipped = load_config()
        @test shipped.data_dir == joinpath(root, "data")
        @test shipped.log_dir == joinpath(root, "logs")
        @test shipped.env_file == joinpath(root, ".env")
    end
end

@testset "config: date forms, non-finite values, repeated channels, provider name" begin
    mktempdir() do dir
        base = "[stream]\nsymbols = [\"A\"]\n"
        p = joinpath(dir, "c.toml")
        rejected(body) = (write(p, body); @test_throws ArgumentError load_config(p))
        loaded(body) = (write(p, body); load_config(p))
        # A date names its year, month and day: Julia's parser alone reads
        # "2026" as 1 January, "2026-08" as the first of the month and
        # "20260812" as a year.
        for s in ("2026", "2026-08", "20260812", "2026-13-01", "2026-08-02T00:00")
            rejected(base * "[backfill]\nstart_date = \"$s\"\nend_date = \"$s\"\n")
        end
        unpadded = loaded(
            base * "[backfill]\nstart_date = \"2026-8-2\"\nend_date = \"2026-8-2\"\n",
        )
        @test unpadded.backfill_start == Date(2026, 8, 2)
        rejected(base * "[backfill]\nstart_date = \"today-99999999999999999999d\"\n")
        rejected(base * "[backfill]\nstart_date = \"today\\n\"\n")      # a trailing newline
        cfg = loaded(
            base * "[backfill]\nstart_date = \"2026-08-02\"\nend_date = \"2026-08-03\"\n",
        )
        @test (cfg.backfill_start, cfg.backfill_end) == (Date(2026, 8, 2), Date(2026, 8, 3))
        # an interval that is slept or drawn: `inf`, and what no timer counts
        rejected(base * "[backfill]\nrate_limit_sleep_s = inf\n")
        rejected(base * "[backfill]\nrate_limit_sleep_s = 1e17\n")
        rejected(base * "[monitor]\nrefresh_s = inf\nrate_window_s = inf\n")
        rejected(base * "[monitor]\nrate_window_s = inf\n")
        @test loaded(base * "[backfill]\nrate_limit_sleep_s = 1e9\n").backfill_rate_sleep_s ==
              1e9
        # the default feed of each shipped provider and source
        binance = "[provider]\nname = \"binance\"\n"
        @test loaded(base).backfill_feed == "sip"
        @test loaded(binance * base).backfill_feed == "aggTrade"
        @test loaded(binance * base * "[backfill]\nsource = \"bulk\"\n").backfill_feed ==
              "aggTrade"
        @test loaded(base * "[replay]\nmax_gap_s = inf\n").replay_max_gap_s == Inf
        # a channel named twice is subscribed to once
        cfg = loaded(
            "[stream]\nsymbols = [\"A\"]\nchannels = [\"trades\", \"bars\", \"trades\"]\n",
        )
        @test cfg.channels == [:trades, :bars]
        # an unknown provider is reported under its key
        write(p, "[provider]\nname = \"kraken\"\n" * base)
        @test_throws "provider.name" load_config(p)
        write(p, "[stream]\nsymbols = [" * join(("\"S$i\"" for i in 1:31), ", ") * "]\n")
        @test_throws "stream.symbols" load_config(p)
    end
    @test_throws "refresh_s must be in" monitor_raw(mktempdir(); refresh_s = Inf)
    @test_throws "rate_window_s must be at most" monitor_raw(
        mktempdir();
        rate_window_s = Inf,
    )
end

@testset "scripts: argument parsing" begin
    cli = Module(:ScriptsCli)
    Base.include(cli, joinpath(@__DIR__, "..", "scripts", "cli.jl"))
    parse_cli(args; kw...) = example_call(cli, :parse_cli, args; kw...)
    config_path(args...) = example_call(cli, :config_path, args...)
    r = parse_cli(
        ["a.jsonl", "--config", "c.toml", "--full", "b.jsonl"];
        valued = ("config",),
        flags = ("full",),
    )
    @test r.opts == Dict("config" => "c.toml") && r.flags == Set(["full"])
    @test r.positional == ["a.jsonl", "b.jsonl"]
    @test parse_cli(["--config=c.toml"]; valued = ("config",)).opts["config"] == "c.toml"
    @test_throws ArgumentError parse_cli(["--config"]; valued = ("config",))
    @test_throws ArgumentError parse_cli(["--full=1"]; flags = ("full",))
    @test_throws ArgumentError parse_cli(["--nope"])
    @test config_path(Dict("config" => "c.toml")) == "c.toml"
    @test config_path(Dict{String,String}(), ["p.toml"]) == "p.toml"
    @test_throws ArgumentError config_path(Dict("config" => "c.toml"), ["p.toml"])
    @test_throws ArgumentError config_path(Dict{String,String}(), ["a.toml", "b.toml"])
end
