# Terminal plots for the attach-mode monitor. `monitor_raw` looks this module
# up with `Base.get_extension` and draws through it when UnicodePlots is
# loaded; without it the dashboard stays text.

module MarketTickStreamerUnicodePlotsExt

using UnicodePlots: barplot, lineplot

function draw_rate_history(io::IO, rates::AbstractVector{<:Real}, rate_window_s::Real)
    println(
        io,
        lineplot(
            rates;
            title = "Ticks/s (window $(round(Int, rate_window_s)) s)",
            height = 6,
            width = 54,
        ),
    )
    return nothing
end

function draw_symbol_counts(
    io::IO,
    symbols::AbstractVector{<:AbstractString},
    counts::AbstractVector{<:Integer},
)
    println(io, barplot(symbols, counts; title = "Ticks by symbol"))
    return nothing
end

end # module
