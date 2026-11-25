@testset "sinks: NDJSON round-trip, rolling, recovery" begin
    t = sample_trade(1)
    @test json_to_trade(trade_to_json(t)) == t
    mktempdir() do dir
        sink = open_raw_sink(dir, "sess"; max_mb = 1)
        write_batch!(sink, [sample_trade(i) for i in 1:100])
        close_sink!(sink)
        # never reopen: a second sink with the same prefix gets a new part
        sink2 = open_raw_sink(dir, "sess"; max_mb = 1)
        @test sink2.path != sink.path
        close_sink!(sink2)
        @test !isfile(sink2.path)          # never written: removed at close
        trades = read_raw(sink.path)
        @test length(trades) == 100
        @test trades[7] == sample_trade(7)
        # corrupt-line recovery: truncate mid-line, append garbage
        open(sink.path, "a") do io
            print(io, "{\"symbol\":\"AAPL\",\"time_ns\":123")   # no newline, cut off
        end
        @test length(read_raw(sink.path)) == 100
    end
end

@testset "sinks: run_sink! batching via channel close" begin
    mktempdir() do dir
        sink = open_raw_sink(dir, "batch")
        ch = Channel{Trade}(1000)
        task = Threads.@spawn run_sink!(
            ch,
            sink;
            flush_interval_s = 10.0,
            flush_max_ticks = 25,
        )
        for i in 1:60
            put!(ch, sample_trade(i))
        end
        close(ch)                       # shutdown signal → final flush
        @test fetch(task) == 60
        close_sink!(sink)
        @test length(read_raw(sink.path)) == 60
    end
    # Closed while a put! waits on the full channel (stop!'s bound on a
    # stuck producer): the print already in it is written, no error.
    mktempdir() do dir
        sink = open_raw_sink(dir, "pending")
        ch = Channel{Trade}(1)
        put!(ch, sample_trade(1))
        pending = @async try
            put!(ch, sample_trade(2))
        catch
        end
        yield()
        close(ch)
        @test run_sink!(ch, sink; flush_interval_s = 0.01) == 1
        wait(pending)
        close_sink!(sink)
        @test read_raw(sink.path) == [sample_trade(1)]
    end
    # Every write is reported as (batch, running total). The channel is full
    # and closed before the loop starts, so the batches are fixed: two at the
    # tick limit, the remainder at the close.
    mktempdir() do dir
        sink = open_raw_sink(dir, "report")
        ch = Channel{Trade}(10)
        foreach(i -> put!(ch, sample_trade(i)), 1:7)
        close(ch)
        seen = Tuple{Int,Int}[]
        n = run_sink!(
            ch,
            sink;
            flush_interval_s = 60.0,
            flush_max_ticks = 3,
            on_flush = (batch, total) -> push!(seen, (batch, total)),
        )
        close_sink!(sink)
        @test n == 7
        @test seen == [(3, 3), (3, 6), (1, 7)]
        @test read_raw(sink.path) == [sample_trade(i) for i in 1:7]
    end
end

@testset "raw parts: a session's parts are listed in the order they were written" begin
    # Part numbers are compared as numbers: past part999 the zero padding no
    # longer orders the names. Other sessions and sidecars are left out.
    mktempdir() do dir
        for f in (
            "s_part999.jsonl",
            "s_part1000.jsonl",
            "s_part1001.jsonl",
            "s_part002.jsonl",
            "s_AAPL_2026-01-02_part001.jsonl",
            "s_AAPL_2026-01-02_part010.jsonl",
            "s_AAPL_2026-01-02_part009.jsonl",
            "other_part001.jsonl",
            "s.meta.toml",
        )
            touch(joinpath(dir, f))
        end
        @test basename.(MarketTickStreamer._raw_files_with_prefix(dir, "s")) == [
            "s_AAPL_2026-01-02_part001.jsonl",
            "s_AAPL_2026-01-02_part009.jsonl",
            "s_AAPL_2026-01-02_part010.jsonl",
            "s_part002.jsonl",
            "s_part999.jsonl",
            "s_part1000.jsonl",
            "s_part1001.jsonl",
        ]
        absent = MarketTickStreamer._raw_files_with_prefix(joinpath(dir, "absent"), "s")
        @test absent == String[] && absent isa Vector{String}
    end
end

@testset "compaction" begin
    mktempdir() do dir
        sink = open_raw_sink(dir, "c")
        write_batch!(
            sink,
            [sample_trade(i; sym = iseven(i) ? "MSFT" : "AAPL") for i in 1:20],
        )
        close_sink!(sink)
        out = joinpath(dir, "processed")
        files = compact_raw([sink.path], out; format = :csv)
        @test length(files) == 2                       # one per symbol, same day
        @test all(isfile, files)
        # safesave: a recompaction of the same input leaves the files as
        # they are; one with other content takes the canonical names, and
        # the files it displaces survive as numbered backups.
        before = Dict(f => read(f) for f in files)
        @test compact_raw([sink.path], out; format = :csv) == files
        @test !any(occursin("_#", f) for (_, _, fs) in walkdir(out) for f in fs)
        extra = open_raw_sink(dir, "c2")
        write_batch!(
            extra,
            [sample_trade(21; sym = "AAPL"), sample_trade(22; sym = "MSFT")],
        )
        close_sink!(extra)
        files2 = compact_raw([sink.path, extra.path], out; format = :csv)
        @test files2 == files
        for f in files
            base, ext = splitext(f)
            @test isfile(f)
            @test read(base * "_#1" * ext) == before[f]
        end
        @test !any(occursin(".partial", f) for (_, _, fs) in walkdir(out) for f in fs)
        arrow_files =
            compact_raw([sink.path, extra.path], joinpath(dir, "arrow"); format = :arrow)
        @test length(arrow_files) == 2
        # Arrow stores the low-cardinality columns dictionary-encoded and
        # reads back the same table the CSV holds, compressed or not.
        zstd_files = compact_raw(
            [sink.path, extra.path],
            joinpath(dir, "zstd");
            format = :arrow,
            compression = :zstd,
        )
        for (c, a, z) in zip(files, arrow_files, zstd_files)
            csv = CSV.read(c, DataFrame; types = Dict(:conditions => String))
            tbl = MarketTickStreamer.Arrow.Table(a)
            @test tbl.conditions isa MarketTickStreamer.Arrow.DictEncoded
            @test tbl.symbol isa MarketTickStreamer.Arrow.DictEncoded
            @test !(tbl.price isa MarketTickStreamer.Arrow.DictEncoded)
            for f in (a, z)
                df = MarketTickStreamer._read_processed(f)
                @test df.time_ns == csv.time_ns && df.price == csv.price
                @test df.symbol == csv.symbol && df.conditions == csv.conditions
            end
        end
        @test_throws ArgumentError compact_raw(
            [sink.path],
            joinpath(dir, "x");
            compression = :zstd,
        )
        @test_throws ArgumentError compact_raw(
            [sink.path],
            joinpath(dir, "x");
            format = :arrow,
            compression = :gzip,
        )
        # Empty text reads back as "", whether the file quotes it or leaves
        # it unquoted, which CSV.jl reads as `missing`.
        t1 = Trade("AAPL", 1_753_886_600_000_000_000, 0, 100.0, 1.0, "V", String[], "C", 1)
        t2 = Trade("AAPL", 1_753_886_600_001_000_000, 0, 100.5, 2.0, "", ["@"], "", 2)
        p = MarketTickStreamer._write_group([t1, t2], dir, "EMPTY", Date(2025, 7, 30), :csv)
        @test MarketTickStreamer._trades_from_processed(p) == [t1, t2]
        write(
            p,
            "symbol,time_ns,recv_ns,price,size,exchange,conditions,tape,id\n" *
            "AAPL,1753886600000000000,0,100.0,1.0,V,,C,1\n" *
            "AAPL,1753886600001000000,0,100.5,2.0,,@,,2\n",
        )
        @test MarketTickStreamer._trades_from_processed(p) == [t1, t2]
    end
end

@testset "compaction: a day known to be incomplete is marked" begin
    mktempdir() do dir
        raw = joinpath(dir, "raw")
        mkpath(raw)
        day1, day2 = Date(2025, 7, 30), Date(2025, 7, 31)   # sample_trade's UTC dates
        shift(t, d) = Trade(
            t.symbol,
            t.time_ns + d * 86_400_000_000_000,
            t.recv_ns + d * 86_400_000_000_000,
            t.price,
            t.size,
            t.exchange,
            t.conditions,
            t.tape,
            t.id,
        )
        part(name, trades) = (
            p = joinpath(raw, name);
            write(p, join(trade_to_json.(trades), "\n") * "\n");
            p
        )
        status(s) = write(joinpath(raw, "sess.meta.toml"), "[session]\nstatus = \"$s\"\n")
        # A running session: AAPL on both days, MSFT on day 1 only, in the
        # older part; the newest part is empty, as just after a roll.
        p1 = part(
            "sess_part001.jsonl",
            [sample_trade(1), sample_trade(2), sample_trade(3; sym = "MSFT")],
        )
        p2 = part(
            "sess_part002.jsonl",
            [shift(sample_trade(4), 1), shift(sample_trade(5), 1)],
        )
        p3 = part("sess_part003.jsonl", Trade[])
        parts = [p1, p2, p3]
        marker(out, sym, d; ext = "csv") = joinpath(out, sym, "$d.$ext.incomplete.toml")
        compact(out; kw...) = compact_raw(parts, out; format = :csv, tz = tz"UTC", kw...)
        out = joinpath(dir, "processed")
        status("running")
        files = compact(out)
        @test basename.(files) == ["2025-07-30.csv", "2025-07-31.csv", "2025-07-30.csv"]
        @test all(
            isfile,
            [
                marker(out, "AAPL", day1),
                marker(out, "AAPL", day2),
                marker(out, "MSFT", day1),
            ],
        )
        @test occursin("is running", TOML.parsefile(marker(out, "AAPL", day1))["reason"])
        # Canonical names and the default listing are as before; asked for
        # whole days, the listing leaves the marked ones out.
        @test length(processed_files(out, "AAPL")) == 2
        @test isempty(processed_files(out, "AAPL"; complete = true))
        # Ended without completing, or with a sidecar that cannot be read:
        # still marked. Completed: the markers go.
        for s in ("interrupted", "failed", "aborted")
            status(s)
            compact(out)
            @test occursin("is $s", TOML.parsefile(marker(out, "MSFT", day1))["reason"])
        end
        write(joinpath(raw, "sess.meta.toml"), "[[")
        compact(out)
        @test occursin("unreadable", TOML.parsefile(marker(out, "AAPL", day2))["reason"])
        status("completed")
        compact(out)
        @test !any(
            isfile,
            [
                marker(out, "AAPL", day1),
                marker(out, "AAPL", day2),
                marker(out, "MSFT", day1),
            ],
        )
        @test length(processed_files(out, "AAPL"; complete = true)) == 2
        # A completed session that read a vendor file cut short.
        write(
            joinpath(raw, "sess.meta.toml"),
            "[session]\nstatus = \"completed\"\n[session.source]\ntruncated = true\n",
        )
        compact(out)
        @test occursin("ends early", TOML.parsefile(marker(out, "AAPL", day1))["reason"])
        # The spill path and Arrow output mark alike; a rewrite with other
        # content displaces the file and leaves the marker in place.
        status("running")
        spill = joinpath(dir, "spill")
        compact_raw(parts, spill; format = :arrow, tz = tz"UTC", mem_fraction = 1e-12)
        @test isfile(joinpath(spill, "AAPL", "2025-07-30.arrow")) &&
              isfile(marker(spill, "AAPL", day1; ext = "arrow"))
        part("sess_part003.jsonl", [sample_trade(6)])
        compact(out)
        @test isfile(joinpath(out, "AAPL", "2025-07-30_#1.csv")) &&
              isfile(marker(out, "AAPL", day1))
        @test basename.(processed_files(out, "AAPL")) ==
              ["2025-07-30.csv", "2025-07-31.csv"]
        # Asked for, whatever the sidecar says; without a sidecar, nothing.
        status("completed")
        asked = joinpath(dir, "asked")
        compact_raw(parts, asked; tz = tz"UTC", incomplete = true)
        @test TOML.parsefile(marker(asked, "AAPL", day1))["reason"] ==
              "marked by the caller"
        lone = joinpath(dir, "lone")
        mkpath(lone)
        cp(p1, joinpath(lone, "other_part001.jsonl"))
        bare = joinpath(dir, "bare")
        compact_raw([joinpath(lone, "other_part001.jsonl")], bare; tz = tz"UTC")
        @test !isfile(marker(bare, "AAPL", day1))
        # Selection is per day: day 1 from the part without a sidecar, day 2
        # from the running session.
        status("running")
        compact_raw([p2], bare; tz = tz"UTC")
        @test basename.(processed_files(bare, "AAPL"; complete = true)) ==
              ["2025-07-30.csv"]
        @test length(processed_files(bare, "AAPL")) == 2
        # The marker precedes the file: a day file that cannot be written
        # leaves the marker behind.
        broken = joinpath(dir, "broken")
        mkpath(joinpath(broken, "AAPL", "2025-07-31.csv"))      # a directory in its way
        @test_throws Exception compact_raw([p2], broken; format = :csv, tz = tz"UTC")
        @test isfile(marker(broken, "AAPL", day2))
        # The other reasons a sidecar gives.
        reason(text) = (
            write(joinpath(raw, "sess.meta.toml"), text);
            compact(out);
            TOML.parsefile(marker(out, "AAPL", day2))["reason"]
        )
        @test occursin("has no status", reason("[session]\nid = \"sess\"\n"))
        @test occursin("without a session table", reason("id = \"sess\"\n"))
        # A part in a missing directory is reported as before.
        @test_throws ArgumentError compact_raw(
            [joinpath(dir, "absent", "s_part001.jsonl")],
            joinpath(dir, "x");
            format = :bad,
        )
        # A backfill marks a day that has not ended on the exchange calendar.
        cfg = load_config(MarketTickStreamer.DEFAULT_CONFIG)
        today = trading_date(now_ns(); tz = cfg.exchange_tz)
        @test MarketTickStreamer._day_not_over(cfg, today)
        @test !MarketTickStreamer._day_not_over(cfg, today - Day(1))
    end
end

@testset "safesave: newest canonical, backups numbered, failed write harmless" begin
    safesave = MarketTickStreamer._safesave
    mktempdir() do dir
        p = joinpath(dir, "x.csv")
        @test safesave(tmp -> write(tmp, "one"), p) == p
        @test read(p, String) == "one"
        @test readdir(dir) == ["x.csv"]                # first write leaves no backup
        safesave(tmp -> write(tmp, "two"), p)
        safesave(tmp -> write(tmp, "three"), p)
        @test read(p, String) == "three"
        @test read(joinpath(dir, "x_#1.csv"), String) == "one"   # oldest first
        @test read(joinpath(dir, "x_#2.csv"), String) == "two"
        # A writer that dies leaves the canonical file as it was, and no debris.
        @test_throws ErrorException safesave(p) do tmp
            write(tmp, "half")
            error("writer died")
        end
        @test read(p, String) == "three"
        @test sort(readdir(dir)) == ["x.csv", "x_#1.csv", "x_#2.csv"]
        # The temporary name keeps the extension last: FileIO infers the
        # format of a figure from it.
        seen = Ref("")
        safesave(tmp -> (seen[] = tmp; write(tmp, "four")), p)
        @test endswith(seen[], ".partial.csv")
        @test dirname(seen[]) == dir
        # Each write has a temporary name of its own.
        other = Ref("")
        safesave(tmp -> (other[] = tmp; write(tmp, "five")), p)
        @test other[] != seen[]
        # The same bytes again leave the file and its backups as they are.
        n_files = length(readdir(dir))
        safesave(tmp -> write(tmp, "five"), p)
        @test length(readdir(dir)) == n_files && read(p, String) == "five"
    end
    # Two writers of one path: no error, no temporary file left, every
    # file complete. Of overlapping writes the earlier result may be
    # lost; the backups claim distinct names.
    mktempdir() do dir
        p = joinpath(dir, "c.csv")
        body(w, i) = "writer $w, write $i\n"^50
        tasks = [Threads.@spawn(for i in 1:100
            safesave(tmp -> write(tmp, body(w, i)), p)
        end) for w in 1:2]
        foreach(wait, tasks)
        files = readdir(dir)
        @test !any(occursin(".partial", f) for f in files)
        writes = Set(body(w, i) for w in 1:2 for i in 1:100)
        @test all(f -> read(joinpath(dir, f), String) in writes, files)
        @test read(p, String) in (body(1, 100), body(2, 100))
    end
    # Without hard links the backup is a copy into a name created
    # exclusively: two writers never share a slot.
    mktempdir() do dir
        p = joinpath(dir, "n.csv")
        write(p, "kept\n")
        nolink(src, dst) = throw(Base.IOError("hardlink: operation not supported", -95))
        tasks = [
            Threads.@spawn([MarketTickStreamer._backup!(p; link = nolink) for _ in 1:50]) for _ in 1:2
        ]
        slots = reduce(vcat, fetch.(tasks))
        @test allunique(slots) && length(slots) == 100
        @test all(f -> read(f, String) == "kept\n", slots)
        @test !MarketTickStreamer._copy_exclusive(p, first(slots))   # taken
        @test read(first(slots), String) == "kept\n"
        # A copy that fails gives its slot up.
        dst = joinpath(dir, "orphan.csv")
        @test_throws SystemError MarketTickStreamer._copy_exclusive(
            joinpath(dir, "absent.csv"),
            dst,
        )
        @test !ispath(dst)
    end
end

@testset "reconcile, lock, RAM guard, spill compaction, lossy tee" begin
    mktempdir() do dir
        # crash-only reconciliation: a "running" sidecar from a dead process
        write(
            joinpath(dir, "dead.meta.toml"),
            """
[session]
id = "dead"
status = "running"
pid = 99999999
[provenance]
hostname = "$(gethostname())"
""",
        )
        write(joinpath(dir, "dead_part001.jsonl"), "")
        @test reconcile_sessions!(dir) == 1
        meta = TOML.parsefile(joinpath(dir, "dead.meta.toml"))
        @test meta["session"]["status"] == "aborted"
        @test haskey(meta["session"], "reconciled_utc")
        @test reconcile_sessions!(dir) == 0              # idempotent
        # single-instance lock
        l1 = acquire_session_lock(dir)
        @test_throws SessionError acquire_session_lock(dir)
        close(l1)
        l2 = acquire_session_lock(dir)                   # released → reacquirable
        close(l2)
        # Identifiers are stamped to the second: one an earlier session's
        # sidecar, raw part or log holds is not taken again; the next
        # second's is.
        write(
            joinpath(dir, "ids.toml"),
            "[stream]\nsymbols = [\"A\"]\n[storage]\ndata_dir = \"tree\"\n" *
            "[logging]\nlog_dir = \"tree/logs\"\n",
        )
        idcfg = load_config(joinpath(dir, "ids.toml"))
        mkpath(idcfg.raw_dir)
        mkpath(idcfg.log_dir)
        calls = Ref(0)
        ids(seq) = (calls[] = 0; () -> seq[calls[]+=1])
        @test MarketTickStreamer._fresh_session_id(ids(["free"]), idcfg) == "free"
        @test calls[] == 1
        for (held, mark) in (
            ("a", joinpath(idcfg.raw_dir, "a.meta.toml")),
            ("b", joinpath(idcfg.raw_dir, "b_part001.jsonl")),
            ("c", joinpath(idcfg.log_dir, "c.log")),
        )
            write(mark, "")
            t = @elapsed sid =
                MarketTickStreamer._fresh_session_id(ids([held, held * "2"]), idcfg)
            @test sid == held * "2" && calls[] == 2 && t < 1.5
        end
        # RAM ceiling fails loudly
        @test_throws ResourceError check_live_heap(0; context = "test")
        @test check_live_heap(1e9) === nothing
    end
    # spill compaction produces the same result as in-memory compaction
    mktempdir() do dir
        sink = open_raw_sink(dir, "sp")
        write_batch!(
            sink,
            [sample_trade(i; sym = iseven(i) ? "MSFT" : "AAPL") for i in 1:40],
        )
        write_batch!(sink, [sample_trade(i) for i in 1:5])   # duplicates
        close_sink!(sink)
        mem = compact_raw([sink.path], joinpath(dir, "mem"); format = :csv)
        spill = compact_raw(
            [sink.path],
            joinpath(dir, "spill");
            format = :csv,
            mem_fraction = 1e-12,
        )            # force the spill path
        @test length(mem) == length(spill) == 2
        for (m, s) in zip(mem, spill)
            @test CSV.read(m, DataFrame) == CSV.read(s, DataFrame)
        end
    end
    # The spill copy is the size of the input, so it goes beside out_dir,
    # never to tempdir() (a RAM-backed tmpfs on systemd distributions), and
    # is refused without room.
    @test MarketTickStreamer._spill_parent("/data/processed", nothing) == "/data/"
    @test MarketTickStreamer._spill_parent("/data/processed/", nothing) == "/data/"
    @test MarketTickStreamer._spill_parent("/data/processed", "/mnt/scratch") ==
          "/mnt/scratch"
    @test_throws ResourceError MarketTickStreamer._ensure_spill_space(
        "/mnt/small",
        10 * 2^30,
        2 * 2^30,
    )
    @test MarketTickStreamer._ensure_spill_space("/mnt/big", 10 * 2^30, 40 * 2^30) ===
          nothing
    # the message names the offending filesystem and the shortfall
    let e = try
            MarketTickStreamer._ensure_spill_space("/mnt/small", 10 * 2^30, 2 * 2^30)
        catch err
            sprint(showerror, err)
        end
        @test contains(e, "/mnt/small") && contains(e, "scratch_dir")
    end
    # scratch_dir is honored end to end: the directory is created where
    # asked, and only the temporary subdirectory inside it is removed.
    mktempdir() do dir
        sink = open_raw_sink(dir, "sc")
        write_batch!(sink, [sample_trade(i) for i in 1:20])
        close_sink!(sink)
        scratch = joinpath(dir, "nested", "scratch")
        files = compact_raw(
            [sink.path],
            joinpath(dir, "out");
            format = :csv,
            mem_fraction = 1e-12,
            scratch_dir = scratch,
        )
        @test length(files) == 1
        @test isdir(scratch)
        @test isempty(filter(startswith("compact_spill_"), readdir(scratch)))
    end
    # lossy tee: persistence output receives everything, saturated lossy
    # output drops instead of stalling the fan-out
    src = Channel{Trade}(100)
    outs = tee(src, 2; capacity = 5, lossy = [false, true])
    keeper = Threads.@spawn collect(outs[1])
    foreach(i -> put!(src, sample_trade(i)), 1:50)
    close(src)
    @test length(fetch(keeper)) == 50
    @test length(collect(outs[2])) <= 5                  # never consumed → capped
end

@testset "spill compaction within a file-handle budget" begin
    day_ns = 86_400 * 1_000_000_000
    mk(i, sym, day) = Trade(
        sym,
        1_753_886_600_000_000_000 + day * day_ns + i * 1_000_000,
        0,
        100.0 + i,
        1.0 * i,
        "V",
        ["@"],
        "C",
        i,
    )
    # Three symbols over two days, interleaved so that with two handles
    # every group is evicted and reopened many times.
    trades = [mk(i, sym, day) for i in 1:40 for day in 0:1 for sym in ("A", "B", "C")]
    mktempdir() do dir
        sink = open_raw_sink(dir, "lru")
        write_batch!(sink, trades)
        close_sink!(sink)
        mem = compact_raw([sink.path], joinpath(dir, "mem"); format = :csv)
        spill = compact_raw(
            [sink.path],
            joinpath(dir, "spill");
            format = :csv,
            mem_fraction = 1e-12,
            max_open_spill_files = 2,
        )
        @test length(mem) == length(spill) == 6
        @test all(read(a) == read(b) for (a, b) in zip(mem, spill))
        @test !any(startswith("compact_spill_"), readdir(dir))   # scratch removed
        pool = MarketTickStreamer.SpillPool(mktempdir(dir), 2)
        for key in
            (("A", Date(2026, 1, 1)), ("B", Date(2026, 1, 1)), ("C", Date(2026, 1, 1)))
            println(MarketTickStreamer._spill_handle!(pool, key), "x")
            @test length(pool.handles) <= 2
        end
        @test length(pool.seen) == 3
        @test !haskey(pool.handles, ("A", Date(2026, 1, 1)))      # least recently written
        MarketTickStreamer._close_all!(pool)
        @test isempty(pool.handles)
        for bad in (
            (; mem_fraction = 0.0),
            (; mem_fraction = 1.5),
            (; footprint_factor = 0.0),
            (; spill_headroom = 0.5),
            (; max_open_spill_files = 0),
        )
            @test_throws ArgumentError compact_raw([sink.path], joinpath(dir, "x"); bad...)
        end
        # The configuration-driven form files under the provider's calendar.
        p = joinpath(dir, "c.toml")
        write(
            p,
            "[provider]\nname = \"binance\"\nfeed = \"aggTrade\"\n[stream]\nsymbols = [\"A\"]\n" *
            "[storage]\ndata_dir = \"out\"\n[backfill]\nfeed = \"aggTrade\"\npage_limit = 1000\n" *
            "[limits]\nmax_open_spill_files = 2\n",
        )
        cfg = load_config(p)
        written = compact_raw(cfg, [sink.path])
        @test all(startswith(cfg.processed_dir), written)
        # 2025-07-30 14:43 UTC: one UTC date per day offset, as under New York here
        @test length(written) == 6
    end
end

@testset "compaction: a day rewritten in the other format keeps one file" begin
    mktempdir() do dir
        sink = open_raw_sink(dir, "fmt")
        write_batch!(sink, [sample_trade(i) for i in 1:5])
        close_sink!(sink)
        out = joinpath(dir, "processed")
        csv = only(compact_raw([sink.path], out; format = :csv, incomplete = true))
        @test isfile(csv * ".incomplete.toml")
        arrow = only(compact_raw([sink.path], out; format = :arrow))
        day = first(splitext(basename(csv)))
        # the CSV went to its own backups, with its marker
        @test sort(readdir(dirname(csv))) == ["$(day).arrow", "$(day)_#1.csv"]
        @test processed_files(out, "AAPL") == [arrow]
        @test length(read_processed(out, "AAPL")) == 5
        @test only(corpus_inventory(out).n_prints) == 5
        # A tree that holds a day in both formats is refused by every reader of
        # the listing: read as it stands, the day would count twice.
        # Backdated, so the copy below is the newer file at any clock resolution.
        run(`touch -d "1 minute ago" $arrow`)
        cp(joinpath(dirname(csv), "$(day)_#1.csv"), csv)
        @test_throws IntegrityError processed_files(out, "AAPL")
        @test_throws IntegrityError read_processed(out, "AAPL")
        @test_throws IntegrityError corpus_inventory(out)
        # whatever the selection, and naming the newer of the two
        @test_throws "the .csv file is the newer" processed_files(
            out,
            "AAPL";
            from = Date(day) + Day(1),
            complete = true,
        )
        # A marker sorted between the two names does not hide the pair.
        between = joinpath(dirname(csv), "$(day).arrow.incomplete.toml")
        write(between, "reason = \"x\"\n")
        @test_throws IntegrityError processed_files(out, "AAPL")
        rm(between)
        # Back to CSV and to Arrow again: each superseded file takes the next
        # free backup of its own format.
        rm(csv)
        compact_raw([sink.path], out; format = :csv)
        compact_raw([sink.path], out; format = :arrow)
        @test sort(readdir(dirname(csv))) ==
              ["$(day).arrow", "$(day)_#1.arrow", "$(day)_#1.csv", "$(day)_#2.csv"]
    end
    # A tree that already holds both formats of a day is repaired by compacting
    # the day again: the Arrow write retires the CSV, and the listing accepts it.
    mktempdir() do dir
        sink = open_raw_sink(dir, "both")
        write_batch!(sink, [sample_trade(i) for i in 1:5])
        close_sink!(sink)
        out = joinpath(dir, "processed")
        csv = only(compact_raw([sink.path], out; format = :csv))
        aside = only(compact_raw([sink.path], joinpath(dir, "aside"); format = :arrow))
        day = first(splitext(basename(csv)))
        arrow = joinpath(dirname(csv), "$(day).arrow")
        cp(aside, arrow)
        @test isfile(csv) && isfile(arrow)
        compact_raw([sink.path], out; format = :arrow)
        canonical = filter(in(("$(day).csv", "$(day).arrow")), readdir(dirname(csv)))
        @test canonical == ["$(day).arrow"]
        @test processed_files(out, "AAPL") == [arrow]
    end
end

@testset "spill compaction: a group's scratch copy goes before its day file is written" begin
    day_ns = 86_400 * 1_000_000_000
    mk(i, day) = Trade(
        "A",
        1_753_886_600_000_000_000 + day * day_ns + i * 1_000_000,
        0,
        100.0,
        1.0,
        "V",
        ["@"],
        "C",
        i,
    )
    mktempdir() do dir
        sink = open_raw_sink(dir, "scratch")
        write_batch!(sink, [mk(i, day) for day in 0:2 for i in 1:20])
        close_sink!(sink)
        scratch = joinpath(dir, "scratch")
        mkpath(scratch)
        # The `incomplete` callback is asked about each group once it is loaded
        # and before it is written: the scratch directory then holds the groups
        # still to come.
        left = Int[]
        spill_dir() = joinpath(scratch, only(readdir(scratch)))
        written = MarketTickStreamer._compact_spill(
            [sink.path],
            joinpath(dir, "out");
            format = :csv,
            dedup = true,
            tz = tz"UTC",
            max_live_heap_mb = nothing,
            scratch_dir = scratch,
            spill_headroom = 1.1,
            max_open_spill_files = 4,
            compression = :none,
            incomplete = (sym, date) ->
                (push!(left, length(readdir(spill_dir()))); nothing),
            part_reasons = Dict{String,String}(),
        )
        @test length(written.files) == 3 && written.rows == 60
        @test left == [2, 1, 0]
        @test isempty(readdir(scratch))
        # A group over the heap ceiling is refused before its day file exists.
        @test_throws ResourceError MarketTickStreamer._compact_spill(
            [sink.path],
            joinpath(dir, "refused");
            format = :csv,
            dedup = true,
            tz = tz"UTC",
            max_live_heap_mb = 0,
            scratch_dir = scratch,
            spill_headroom = 1.1,
            max_open_spill_files = 4,
            compression = :none,
            incomplete = MarketTickStreamer._no_day,
            part_reasons = Dict{String,String}(),
        )
        @test !isdir(joinpath(dir, "refused", "A"))
    end
end

@testset "session lock: a starter displaced while it settles gives way" begin
    mktempdir() do dir
        lockfile = joinpath(dir, ".session.lock")
        function stale()
            write(lockfile, "99999999 $(gethostname())")
            run(`touch -d "10 minutes ago" $lockfile`)
            return nothing
        end
        # Alone, the starter that breaks a stale lock keeps the tree.
        stale()
        held = Base.CoreLogging.with_logger(Base.CoreLogging.NullLogger()) do
            acquire_session_lock(dir; settle_s = 0.05)
        end
        @test startswith(read(lockfile, String), "$(getpid()) ")
        close(held)
        @test !isfile(lockfile)
        # Pidfile breaks a stale lock by unlinking the name, and a second
        # starter that judged the same file stale unlinks the first one's new
        # lock. The first then finds another pid under the name and yields,
        # leaving that file where it is.
        stale()
        starter = @async Base.CoreLogging.with_logger(Base.CoreLogging.NullLogger()) do
            acquire_session_lock(dir; settle_s = 3.0)
        end
        @test timedwait(
            () -> isfile(lockfile) && startswith(read(lockfile, String), "$(getpid()) "),
            10.0,
        ) === :ok
        rm(lockfile)
        write(lockfile, "1 $(gethostname())")
        err = try
            fetch(starter)
            nothing
        catch e
            e
        end
        @test err isa TaskFailedException && err.task.exception isa SessionError
        @test read(lockfile, String) == "1 $(gethostname())"
        # The file under the name is told from the starter's own by its inode:
        # one written by another task of this process carries the same pid.
        settling(s) = @async Base.CoreLogging.with_logger(Base.CoreLogging.NullLogger()) do
            acquire_session_lock(dir; settle_s = s)
        end
        taken() = isfile(lockfile) && startswith(read(lockfile, String), "$(getpid()) ")
        failure(task) =
            try
                fetch(task)
                nothing
            catch e
                e.task.exception
            end
        stale()
        starter = settling(3.0)
        @test timedwait(taken, 10.0) === :ok
        rm(lockfile)
        write(lockfile, "$(getpid()) $(gethostname())")
        @test failure(starter) isa SessionError
        @test isfile(lockfile)
        # An interrupt during the pause closes the monitor, and the tree is free.
        stale()
        starter = settling(60.0)
        @test timedwait(taken, 10.0) === :ok
        schedule(starter, InterruptException(); error = true)
        @test failure(starter) isa InterruptException
        @test !isfile(lockfile)
        close(acquire_session_lock(dir))
        # No lock file before the call: nothing was replaced, so no pause.
        @test !ispath(lockfile)
        t = @elapsed held = acquire_session_lock(dir; settle_s = 5.0)
        @test t < 2.0
        close(held)
        # A takeover is reported.
        stale()
        held = @test_logs (:warn, r"took the data tree over") match_mode = :any begin
            acquire_session_lock(dir; settle_s = 0.05)
        end
        close(held)
        # A name removed under a held lock no longer holds it, and the release
        # copes with the missing file (Pidfile warns that it removed nothing).
        held = acquire_session_lock(dir)
        @test MarketTickStreamer._still_holds(held, lockfile)
        rm(lockfile)
        @test !MarketTickStreamer._still_holds(held, lockfile)
        @test Base.CoreLogging.with_logger(Base.CoreLogging.NullLogger()) do
            close(held)
            true
        end
    end
end

@testset "provenance: an installed copy records no git state" begin
    # `git -C` searches upward: a depot inside some work tree would otherwise
    # lend that repository's commit to the sidecar.
    @test MarketTickStreamer._git_state(; installed = true) ==
          (; commit = "unknown", dirty = false, available = false)
    here = MarketTickStreamer._git_state(; installed = false)
    @test here.available == (here.commit != "unknown")
end
