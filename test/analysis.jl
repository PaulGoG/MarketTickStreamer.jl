@testset "resampling: transaction, volume and value clocks" begin
    mk(id, price, size; t = id, sym = "AAPL") =
        Trade(sym, Int64(t), 0, price, size, "V", String[], "C", id)
    ts = [mk(1, 10.0, 1.0), mk(2, 11.0, 1.0), mk(3, 9.0, 1.0), mk(4, 12.0, 1.0)]

    b = tick_bars(ts, 2)
    @test length(b) == 2
    @test b[1].open == 10.0 && b[1].close == 11.0
    @test b[1].high == 11.0 && b[1].low == 10.0
    @test b[1].trade_count == 2 && b[1].volume == 2.0
    @test b[1].time_ns == 1                       # bar opens at its first print
    @test b[1].close_ns == 2                      # and closes at the print completing it
    @test b[2].time_ns == 3 && b[2].close_ns == 4
    @test all(b[i].close_ns <= b[i+1].time_ns for i in 1:(length(b)-1))
    @test b[1].vwap == 10.5
    @test b[2].open == 9.0 && b[2].close == 12.0
    @test b[2].low == 9.0 && b[2].high == 12.0
    @test all(x -> x.recv_ns == 0, b)             # derived: never crossed the wire

    # A trailing bar short of its threshold is not comparable to the rest.
    @test length(tick_bars(ts, 3)) == 1
    @test length(tick_bars(ts, 3; keep_partial = true)) == 2
    @test tick_bars(ts, 3; keep_partial = true)[2].trade_count == 1
    partial = tick_bars(ts, 3; keep_partial = true)[2]
    @test partial.time_ns == partial.close_ns == 4    # a one-print bar has no duration

    # Volume clock: 3 + 3 closes the first bar, the remaining 3 is partial.
    vs = [mk(1, 10.0, 3.0), mk(2, 10.0, 3.0), mk(3, 10.0, 3.0)]
    @test length(volume_bars(vs, 6.0)) == 1
    @test volume_bars(vs, 6.0)[1].volume == 6.0
    @test volume_bars(vs, 6.0; keep_partial = true)[2].volume == 3.0

    # Value clock weights each print by price: 100 | 1 + 100.
    ds = [mk(1, 100.0, 1.0), mk(2, 1.0, 1.0), mk(3, 100.0, 1.0)]
    db = dollar_bars(ds, 100.0)
    @test length(db) == 2
    @test db[1].trade_count == 1 && db[2].trade_count == 2

    # Bars are built on exchange time, not arrival order.
    shuffled = [mk(2, 11.0, 1.0; t = 2), mk(1, 10.0, 1.0; t = 1)]
    @test tick_bars(shuffled, 2)[1].open == 10.0
    @test tick_bars(shuffled, 2)[1].close == 11.0

    @test tick_bars(Trade[], 2) == Bar[]
    @test_throws ArgumentError tick_bars(ts, 0)
    @test_throws ArgumentError volume_bars(ts, -1.0)
    # Decimal values: a hundred prints of 10.01 sum to 1000.9999999999993 in
    # binary, and still complete a bar of 1001 in value.
    dec = [Trade("AAPL", Int64(i), 0, 10.01, 1.0, "V", String[], "C", i) for i in 1:100]
    @test length(dollar_bars(dec, 1001.0)) == 1
    @test length(
        volume_bars(fill(Trade("AAPL", 1, 0, 1.0, 0.3, "V", String[], "C", 1), 3), 0.9),
    ) == 1
    @test_throws ArgumentError dollar_bars(ts, 0.0)
    @test_throws ArgumentError tick_bars(
        [mk(1, 1.0, 1.0), mk(2, 1.0, 1.0; sym = "MSFT")],
        1,
    )
end

@testset "resampling: matching events" begin
    mk(id, t, price, size; sym = "BTCUSDT") =
        Trade(sym, Int64(t), 0, price, size, "BINANCE", ["buy"], "SPOT", id)
    # A taker order filled against three resting orders is three prints at
    # one instant; the next two instants hold one and two fills.
    ts = [
        mk(1, 100, 10.0, 1.0),
        mk(2, 100, 10.5, 2.0),
        mk(3, 100, 11.0, 1.0),
        mk(4, 250, 9.0, 4.0),
        mk(5, 400, 9.5, 1.0),
        mk(6, 400, 9.5, 1.0),
    ]
    ev = event_bars(ts)
    @test length(ev) == 3
    @test [e.time_ns for e in ev] == [100, 250, 400]
    @test all(e.time_ns == e.close_ns for e in ev)      # an event has no duration
    @test ev[1].open == 10.0 && ev[1].close == 11.0
    @test ev[1].high == 11.0 && ev[1].low == 10.0
    @test ev[1].volume == 4.0 && ev[1].trade_count == 3
    @test ev[1].vwap ≈ (10.0 + 21.0 + 11.0) / 4.0 rtol = 1e-12
    @test ev[2].trade_count == 1 && ev[2].vwap == 9.0
    @test ev[3].trade_count == 2 && ev[3].volume == 2.0
    @test all(e.recv_ns == 0 for e in ev)
    # Waiting times between events are the gaps between instants.
    @test diff([e.time_ns for e in ev]) == [150, 150]
    # Exchange time decides the events, arrival order the fill sequence
    # within one: a tie split across the input is still one event.
    parts = [ts[4], ts[1], ts[5], ts[2], ts[6], ts[3]]
    @test event_bars(parts) == ev
    @test event_bars(Trade[]) == Bar[]
    @test_throws ArgumentError event_bars([
        mk(1, 1, 1.0, 1.0),
        mk(2, 1, 1.0, 1.0; sym = "ETHUSDT"),
    ])
end

@testset "snapshots: period boundaries on the exchange clock, final at close" begin
    P = 1_000_000_000                       # one second
    at(τ, i) = Trade("AAPL", τ, 0, 100.0, 1.0, "V", String[], "C", i)
    times = [500, 700, 2100, 2200, 5900] .* 1_000_000
    src = Channel{Trade}(10)
    foreach(((i, τ),) -> put!(src, at(τ, i)), enumerate(times))
    close(src)
    snaps = Tuple{Int64,Int,Bool}[]
    volume = 0.0
    n = snapshots(src, P; on_snapshot = (a, k, f) -> push!(snaps, (a, k, f))) do t
        volume += t.size
    end
    @test n == 5 && volume == 5.0
    @test snaps == [
        (1P, 2, false),
        (2P, 2, false),                      # a window without prints
        (3P, 4, false),
        (4P, 4, false),
        (5P, 4, false),
        (5_900_000_000, 5, true),
    ]
    # A late print folds into the open window; the boundary never retreats.
    src = Channel{Trade}(10)
    foreach(((i, τ),) -> put!(src, at(τ, i)), enumerate([1500, 2500, 1800] .* 1_000_000))
    close(src)
    empty!(snaps)
    snapshots(src, P; on_snapshot = (a, k, f) -> push!(snaps, (a, k, f))) do _
    end
    @test snaps == [(2P, 1, false), (1_800_000_000, 3, true)]
    # The receipt clock, and an empty stream makes no call.
    src = Channel{Trade}(10)
    put!(src, Trade("AAPL", 0, 3_200_000_000, 1.0, 1.0, "V", String[], "C", 1))
    close(src)
    empty!(snaps)
    snapshots(
        src,
        P;
        clock = :recv,
        on_snapshot = (a, k, f) -> push!(snaps, (a, k, f)),
    ) do _
    end
    @test snaps == [(3_200_000_000, 1, true)]
    src = Channel{Trade}(1)
    close(src)
    empty!(snaps)
    @test snapshots(identity, src, P; on_snapshot = (a, k, f) -> push!(snaps, (a, k, f))) ==
          0
    @test isempty(snaps)
    @test_throws ArgumentError snapshots(
        identity,
        Channel{Trade}(1),
        0;
        on_snapshot = identity,
    )
    @test_throws ArgumentError snapshots(
        identity,
        Channel{Trade}(1),
        P;
        on_snapshot = identity,
        clock = :wall,
    )
    # The receive clock needs a receive timestamp; a backfilled print has none.
    let ch = Channel{Trade}(1)
        put!(ch, Trade("AAPL", 1, 0, 1.0, 1.0, "V", String[], "C", 1))
        close(ch)
        @test_throws ArgumentError snapshots(
            _ -> nothing,
            ch,
            10^9;
            on_snapshot = (at, n, final) -> nothing,
            clock = :recv,
        )
    end
end

@testset "diagnostics: clock ticks, decimation, tail fit, formatting" begin
    M = MarketTickStreamer
    # HH:MM domain ticks
    vals, labels = M._hhmm_ticks(9.5, 16.0)
    @test labels[1] == "10:00" && labels[end] == "16:00"
    # a whole day is labeled every four hours, which fits a half-width panel
    @test M._hhmm_ticks(0.0, 24.0)[2] ==
          ["00:00", "04:00", "08:00", "12:00", "16:00", "20:00", "24:00"]
    # Pooled waiting times: a closure between days is not a waiting time,
    # midnight on a venue that never closes is.
    day(d, ts) = (d, DataFrame(time_ns = Int64.(ts) .* 1_000_000_000))
    days = [
        day(Date(2026, 9, 18), [10, 11, 13]),
        day(Date(2026, 9, 19), [20, 24]),
        day(Date(2026, 9, 21), [50, 51]),          # the 20th is missing
    ]
    gaps, excluded = M._pooled_gaps(days; continuous = false)
    @test gaps == [1.0, 2.0, 4.0, 1.0] && excluded == 2
    gaps, excluded = M._pooled_gaps(days; continuous = true)
    @test gaps == [1.0, 2.0, 7.0, 4.0, 1.0] && excluded == 1
    @test M._pooled_gaps(days[1:1]; continuous = true) == ([1.0, 2.0], 0)
    # The overview's distribution panels take the prints inside the session.
    # New York on 2026-07-30 is UTC-4: prints at 08:00, 10:00:00, 10:00:01,
    # 15:00 and 18:00 local, then a day whose one print comes at 20:00.
    ny = tz"America/New_York"
    local_ns(d, h, m, s) =
        Dates.value(DateTime(d) + Hour(h + 4) + Minute(m) + Second(s) - DateTime(1970)) *
        1_000_000
    table(d, clock) = (
        d,
        DataFrame(;
            time_ns = Int64[local_ns(d, c...) for c in clock],
            size = Float64.(eachindex(clock)),
        ),
    )
    d1, d2 = Date(2026, 7, 30), Date(2026, 7, 31)
    extended = [
        table(d1, [(8, 0, 0), (10, 0, 0), (10, 0, 1), (15, 0, 0), (18, 0, 0)]),
        table(d2, [(20, 0, 0)]),
    ]
    inside = M._within_session(extended, (9.5, 16.0); tz = ny)
    @test first.(inside) == [d1, d2]                     # an empty day keeps its place
    @test inside[1][2].size == [2.0, 3.0, 4.0]
    @test isempty(inside[2][2].time_ns)
    @test size(extended[1][2], 1) == 5                   # the input is left as it was
    @test M._pooled_gaps(inside; continuous = false) == ([1.0, 5 * 3600 - 1.0], 1)
    # both bounds belong to the session; the whole day is the input itself
    edge = [table(d1, [(9, 30, 0), (16, 0, 0), (16, 0, 1)])]
    @test M._within_session(edge, (9.5, 16.0); tz = ny)[1][2].size == [1.0, 2.0]
    @test M._within_session(extended, (0.0, 24.0); tz = ny) === extended
    # thinning preserves ends; decimation preserves extrema
    xs = collect(1.0:10_000.0)
    tx, ty = M._thin(xs, xs; cap = 500)
    @test length(tx) <= 810 && tx[1] == 1.0 && tx[end] == 10_000.0
    dx, dy = M._decimate_minmax(xs, sin.(xs); nbins = 50)
    @test length(dx) <= 100
    @test maximum(dy) ≈ maximum(sin.(xs)) atol = 1e-3
    # Hill estimator on the quantiles of a Pareto law with alpha = 2.5
    n = 5000
    x = sort!([((i - 0.5) / n)^(-1 / 2.5) for i in 1:n])
    fit = M._tail_fit(x)
    @test fit !== nothing
    @test isapprox(fit.α, 2.5; rtol = 0.02)
    @test fit.σ_stat ≈ fit.α / sqrt(500) rtol = 1e-12    # k = top 10 %
    # Exact Pareto quantiles: the estimate barely moves with the threshold.
    @test fit.σ_threshold < 0.005 && fit.σ ≈ hypot(fit.σ_stat, fit.σ_threshold)
    @test M._tail_fit(x[1:40]) === nothing               # too few samples
    @test M._tail_fit(fill(100.0, 500)) === nothing      # degenerate tail
    # Survival function: P(X ≥ x) over the positive samples, one point per
    # distinct value.
    @test MarketTickStreamer._ccdf([0.0, 0.0, 0.0, 1.0, 1.0, 2.0]) ==
          ([1.0, 2.0], [1.0, 1 / 3])
    @test MarketTickStreamer._ccdf([100.0, 100.0, 100.0, 200.0]) ==
          ([100.0, 200.0], [1.0, 0.25])
    @test MarketTickStreamer._ccdf([0.0, -1.0]) == (Float64[], Float64[])
    # Ties at the threshold stay out of the Hill sum: 8500 samples below
    # it, 1000 at u = 100 and the 500 quantiles of a Pareto(α = 1.5) tail
    # above. Counted in k, the ties would double the estimate.
    let α = 1.5
        pareto = [100.0 * ((j - 0.5) / 500)^(-1 / α) for j in 1:500]
        tied = sort!(vcat(fill(50.0, 8500), fill(100.0, 1000), pareto))
        fit = MarketTickStreamer._tail_fit(tied)
        @test fit.α ≈ α rtol = 0.02
        @test fit.σ_stat ≈ fit.α / sqrt(500) rtol = 1e-12
        # Threshold term: the top 5 % gives the same 500 exceedances, the
        # top 20 % has u = 50 and all 1500 samples above it, where
        # Σ ln(x/50) = 1500 ln 2 + (500 - ln(2)/2)/α = 1372.823, so
        # α̂ = 1.092639 there against 1.501040 here; the larger shift:
        @test fit.σ_threshold ≈ 0.408401 atol = 1e-5
        @test fit.σ ≈ hypot(fit.σ_stat, fit.σ_threshold) rtol = 1e-12
    end
    # At least 30 samples above the threshold: 30 at 5 over 970 at 1 give
    # α̂ = 1/ln 5, and every threshold tried sits on the same tied value,
    # so the threshold term vanishes; 29 give no fit.
    let fit = MarketTickStreamer._tail_fit(sort!(vcat(fill(1.0, 970), fill(5.0, 30))))
        @test fit.α ≈ 1 / log(5) rtol = 1e-12
        @test fit.σ_stat ≈ fit.α / sqrt(30) rtol = 1e-12
        @test fit.σ_threshold == 0
    end
    @test MarketTickStreamer._tail_fit(sort!(vcat(fill(1.0, 971), fill(5.0, 29)))) ===
          nothing
    # Survival function at a single value and with a tied maximum.
    @test MarketTickStreamer._ccdf([5.0, 5.0, 5.0]) == ([5.0], [1.0])
    @test MarketTickStreamer._ccdf([1.0, 2.0, 2.0]) == ([1.0, 2.0], [1.0, 2 / 3])
    # Zero gaps are reported as a share of all gaps, or not at all.
    @test MarketTickStreamer._zero_gap_note([1.0, 2.0]) === nothing
    @test MarketTickStreamer._zero_gap_note([0.0, 0.0, 0.0, 1.0]) ==
          "Zero gaps (excluded): 75 %"
    @test MarketTickStreamer._zero_gap_note(vcat(0.0, fill(1.0, 39))) ==
          "Zero gaps (excluded): 2.5 %"
    @test MarketTickStreamer._zero_gap_note(vcat(0.0, fill(1.0, 1999))) ==
          "Zero gaps (excluded): < 0.1 %"
    # 9.95 % rounds to 10 and is written as the integer.
    @test MarketTickStreamer._zero_gap_note(vcat(zeros(199), ones(1801))) ==
          "Zero gaps (excluded): 10 %"
    # annotations: value and uncertainty share their decimals
    @test M._value_pm(1.4294, 0.0041) == ("1.429", "0.004")
    @test M._value_pm(1.8676, 0.0684) == ("1.87", "0.07")
    @test M._value_pm(1.4, 0.2) == ("1.4", "0.2")
    @test M._si_seconds(4.6e-4) == "460 μs"
    @test M._si_seconds(2.5) == "2.5 s"
    # The unit follows the value as rendered, and no exponent is printed.
    @test M._si_seconds(0.99996) == "1 s"
    @test M._si_seconds(9.9996e-7) == "1 μs"
    @test M._si_seconds(9.994e-4) == "999 μs"
    @test M._si_seconds(1234.5) == "1230 s"
    @test M._si_seconds(3.2e-10) == "0.32 ns"
    @test M._si_seconds(0.0) == "0 ns"
end

@testset "figures: without CairoMakie the stubs say what to load" begin
    @test Base.get_extension(MarketTickStreamer, :MarketTickStreamerMakieExt) === nothing
    for call in (
        () -> session_figure([sample_trade(1)]),
        () -> tick_theme(),
        () -> save_session_figures(String[], mktempdir()),
    )
        err = try
            call()
        catch e
            e
        end
        @test err isa MethodError
        @test occursin("load CairoMakie", sprint(showerror, err))
    end
    # The hint is specific to the figure functions.
    err = try
        price_forming(1)
    catch e
        e
    end
    @test err isa MethodError && !occursin("CairoMakie", sprint(showerror, err))
end
