using CairoMakie: CairoMakie
using UnicodePlots: UnicodePlots

@testset "MarketTickStreamer extensions" begin
    makie_ext = Base.get_extension(MarketTickStreamer, :MarketTickStreamerMakieExt)
    plots_ext = Base.get_extension(MarketTickStreamer, :MarketTickStreamerUnicodePlotsExt)

    @testset "both load with their trigger" begin
        @test makie_ext isa Module
        @test plots_ext isa Module
    end

    @testset "static QA (ExplicitImports)" begin
        # Owner checks are not asserted for the figure extension: the plotting
        # surface is re-exported (Makie names through CairoMakie, `save`
        # through FileIO, `@L_str` through LaTeXStrings).
        for ext in (makie_ext, plots_ext)
            @test check_no_implicit_imports(ext) === nothing
            @test check_no_stale_explicit_imports(ext) === nothing
            @test check_no_self_qualified_accesses(ext) === nothing
        end
    end

    @testset "static QA (JET)" begin
        # `report_package` does not reach extension modules, so their entry
        # points are analyzed call by call.
        if JET_ENABLED
            trades = [sample_trade(i) for i in 1:60]
            JET.@test_call target_modules = (makie_ext,) tick_theme()
            JET.@test_call target_modules = (makie_ext,) session_figure(trades)
            JET.@test_call target_modules = (makie_ext,) save_session_figures(
                String[],
                mktempdir(),
            )
            JET.@test_call target_modules = (plots_ext,) plots_ext.draw_rate_history(
                IOBuffer(),
                [1.0, 2.0],
                300.0,
            )
            JET.@test_call target_modules = (plots_ext,) plots_ext.draw_symbol_counts(
                IOBuffer(),
                ["AAPL"],
                [1],
            )
        else
            @test_skip "JET analyses of the extensions (JET unavailable)"
        end
    end

    @testset "figures: axis labeling" begin
        E = makie_ext
        # log ticks: plain decimals on short spans, exponent + collapse on long
        v, l = E._log_ticks(0.5, 80.0)
        @test "1" in l &&
              "2" in l &&
              "50" in l &&
              !any(occursin("10^", string(x)) for x in l)
        v, l = E._log_ticks(1e-7, 10.0)
        @test "1" in l && "10" in l                          # 10^0 and 10^1 collapse
        @test any(x -> occursin("10^{-6}", string(x)), l)
        # intermediates carry their own mantissa, never the rounded power of ten
        v, l = E._log_ticks(3e-6, 2e-4)
        @test allunique(string.(l))
        @test any(x -> occursin("2\\times10^{-5}", string(x)), l)
        @test E._pow10_label(5, 0) == "5" && E._pow10_label(2, 1) == "20"
        @test E._pow10_label(2, -1) == "0.2" && E._pow10_label(1, 1) == "10"
        v, l = E._log_ticks(1.3e-4, 1.0)                     # four orders: 10^k only
        @test length(v) == 4 && allunique(string.(l))
        # one exponent per axis: the rate is rescaled, not the tick labels
        @test E._rate_scale(2.2e4)[1] == 1e3
        @test E._rate_scale(800)[1] == 1.0
    end

    @testset "figures: theme, layout, session window, files" begin
        theme = tick_theme()
        @test theme isa CairoMakie.Makie.Attributes
        @test theme.fontsize[] == 26
        @test haskey(theme, :fonts)                          # Computer Modern faces
        trades = [sample_trade(i) for i in 1:200]
        @test session_figure(trades) isa CairoMakie.Figure
        df = DataFrame(
            symbol = fill("AAPL", 100),
            time_ns = [1_753_886_600_000_000_000 + i * 10_000_000_000 for i in 1:100],
            recv_ns = zeros(Int64, 100),
            price = 100.0 .+ sin.(1:100),
            size = Float64.(mod1.(7 .* (1:100), 100)),
            exchange = fill("V", 100),
            conditions = fill("@", 100),
            tape = fill("C", 100),
            id = collect(1:100),
        )
        days = [(Date(2026, 7, 30), df), (Date(2026, 7, 31), df)]
        @test overview_figure("AAPL", days) isa CairoMakie.Figure
        # A venue that never closes lays its days out on the whole UTC day.
        @test overview_figure("BTCUSDT", days; tz = tz"UTC", session = (0.0, 24.0)) isa
              CairoMakie.Figure
        # Thirty days carry ten labels on either day axis.
        month = [(Date(2026, 7, 1) + Day(i), df) for i in 0:29]
        axes = filter(x -> x isa CairoMakie.Axis, overview_figure("AAPL", month).content)
        @test length(axes[1].xticks[][1]) == 10 && length(axes[2].yticks[][1]) == 10
        # A print before the open is not on the price path: the session starts
        # at 09:30 New York, and 07:43 lies outside it.
        early = copy(df)
        early.time_ns[1] -= 3 * 3600 * 10^9
        early.price[1] = 500.0
        ax = first(overview_figure("AAPL", [(Date(2026, 7, 30), early)]).content)
        @test ax.limits[][2][2] < 200
        # The two distribution panels pool the session as well: that print,
        # 3 h ahead of the next one and of size 1e6 here, adds neither a
        # waiting time nor a size. On the whole day it adds both.
        early.size[1] = 1.0e6
        panels(; kw...) = filter(
            x -> x isa CairoMakie.Axis,
            overview_figure("AAPL", [(Date(2026, 7, 30), early)]; kw...).content,
        )
        session_panels = panels()
        @test maximum(session_panels[3].xticks[][1]) == 100      # gaps of 10 s
        @test maximum(session_panels[4].xticks[][1]) == 100      # sizes 1 to 100
        day_panels = panels(; session = (0.0, 24.0))
        @test maximum(day_panels[3].xticks[][1]) == 1.0e4        # one gap of 10 810 s
        @test maximum(day_panels[4].xticks[][1]) == 1.0e6
        # A day with no print inside the session draws nothing and keeps its
        # slot on the axis: two days of 6.5 h, 0.25 h apart, and a 2 % margin.
        before_open = copy(df)
        before_open.time_ns .-= 5 * 3600 * 10^9          # 05:43 New York onward
        @test overview_figure("AAPL", [(Date(2026, 7, 29), before_open)]) isa
              CairoMakie.Figure
        ax = first(
            overview_figure(
                "AAPL",
                [(Date(2026, 7, 29), before_open), (Date(2026, 7, 30), df)],
            ).content,
        )
        @test collect(ax.limits[][1]) ≈ [-0.27, 13.25 + 0.27]
        # One price path per day that has a print inside the session.
        paths(fig) = count(p -> p isa CairoMakie.Lines, first(fig.content).scene.plots)
        @test paths(overview_figure("AAPL", [(Date(2026, 7, 29), before_open)])) == 0
        @test paths(
            overview_figure(
                "AAPL",
                [(Date(2026, 7, 29), before_open), (Date(2026, 7, 30), df)],
            ),
        ) == 1
        # A session whose prints are all odd lots is drawn from them, also when
        # a price-forming print lies before the open.
        odd = copy(df)
        odd.conditions .= "I"
        odd.conditions[1] = "@"
        odd.time_ns[1] -= 5 * 3600 * 10^9
        @test paths(overview_figure("AAPL", [(Date(2026, 7, 30), odd)])) == 1
        @test_throws ArgumentError overview_figure("AAPL", days; session = (16.0, 9.5))
        @test_throws ArgumentError overview_figure("AAPL", days; session = (0.0, 25.0))
        @test provider_spec("alpaca").regular_session == (9.5, 16.0)
        @test provider_spec("binance").regular_session == (0.0, 24.0)
        @test provider_spec("alpaca").size_unit == "shares"
        @test provider_spec("binance").price_unit == "quote asset"
        @test session_figure(
            trades;
            price_unit = "quote asset",
            size_unit = "base asset",
        ) isa CairoMakie.Figure
        # Files land under their canonical names, with no partial left behind.
        mktempdir() do dir
            sink = open_raw_sink(dir, "fig")
            write_batch!(sink, trades)
            close_sink!(sink)
            out = joinpath(dir, "plots")
            written =
                save_session_figures([sink.path], out; formats = ("png",), min_trades = 10)
            @test length(written) == 1 && all(isfile, written)
            @test readdir(out) == basename.(written)
            # The overview is drawn from the processed tree.
            two_days = [
                Trade(
                    t.symbol,
                    t.time_ns + d * 86_400_000_000_000,
                    0,
                    t.price,
                    t.size,
                    t.exchange,
                    t.conditions,
                    t.tape,
                    t.id,
                ) for d in 0:1 for t in trades
            ]
            sink = open_raw_sink(dir, "days")
            write_batch!(sink, two_days)
            close_sink!(sink)
            processed = joinpath(dir, "processed")
            compact_raw([sink.path], processed; tz = tz"UTC")
            overview = save_overview_figures(
                processed,
                joinpath(dir, "overview");
                formats = ("png",),
                tz = tz"UTC",
                session = (0.0, 24.0),
            )
            @test basename.(overview) == ["AAPL_2025-07-30_2025-07-31.png"]
            @test all(isfile, overview)
        end
    end

    @testset "monitor: terminal plots" begin
        mktempdir() do dir
            sink = open_raw_sink(dir, "mon")
            write_batch!(sink, [sample_trade(i) for i in 1:30])
            close_sink!(sink)
            buf = IOBuffer()
            # Three refreshes: the first read holds the backlog and carries no
            # rate, and the history needs two rates.
            monitor_raw(dir; refresh_s = 0.01, iterations = 3, from_start = true, io = buf)
            out = String(take!(buf))
            @test occursin("Ticks by symbol", out) && occursin("┌", out)   # bar chart frame
            @test occursin("Ticks/s", out)                                  # rate history
        end
    end
end
