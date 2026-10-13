# Alternative clocks: sampling the flux by activity rather than by the
# calendar.
#
# Market activity is not uniform in time, so calendar-time sampling draws unevenly from the
# process that generates prices — densely at the open, sparsely at lunch. Sampling instead
# once per fixed count of prints, of shares, or of traded value subordinates the price
# process to a clock that runs with the market. Under such a clock, returns are closer to
# independent and to normality than under wall-clock sampling, which is the empirical result
# that motivates the whole family (Mandelbrot & Taylor 1967, doi:10.1287/opre.15.6.1057;
# Clark 1973, doi:10.2307/1913889; Ané & Geman 2000, doi:10.1111/0022-1082.00286).
#
# Bars are derived objects: they carry `recv_ns = 0`, the same marker the raw
# layer uses for records that never crossed the wire. Each bar records both
# ends of its interval, `time_ns` of its first print and `close_ns` of its
# last, since on an activity clock the duration is data.

# The symbol of a single-symbol print vector, and the prints on exchange time.
# A consolidated tape interleaves venues and reports late prints, so arrival
# order is not chronological order; the sort is stable, leaving equal-stamped
# prints in arrival order.
function _one_symbol_ordered(trades::AbstractVector{Trade})
    sym = trades[1].symbol
    all(t -> t.symbol == sym, trades) || throw(
        ArgumentError(
            "resampling expects prints of one symbol; got $(length(unique(t.symbol for t in trades)))",
        ),
    )
    ordered =
        issorted(trades; by = t -> t.time_ns) ? trades :
        sort(trades; by = t -> t.time_ns, alg = MergeSort)
    return sym, ordered
end

function _activity_bars(
    trades::AbstractVector{Trade},
    threshold::Real,
    increment;
    keep_partial::Bool = false,
)
    threshold > 0 || throw(ArgumentError("bar threshold must be positive, got $threshold"))
    isempty(trades) && return Bar[]
    sym, ordered = _one_symbol_ordered(trades)

    bars = Bar[]
    i, n = 1, length(ordered)
    while i <= n
        first_print = ordered[i]
        high = low = first_print.price
        volume = 0.0
        notional = 0.0
        accumulated = 0.0
        count = 0
        j = i
        while j <= n
            t = ordered[j]
            high = max(high, t.price)
            low = min(low, t.price)
            volume += t.size
            notional += t.price * t.size
            count += 1
            accumulated += increment(t)
            j += 1
            # The print that crosses the threshold belongs to the bar it
            # closes, so a bar is never short of its threshold.
            accumulated >= threshold && break
        end
        if accumulated >= threshold || keep_partial
            push!(
                bars,
                Bar(
                    sym,
                    first_print.time_ns,
                    ordered[j-1].time_ns,
                    0,
                    first_print.price,
                    high,
                    low,
                    ordered[j-1].price,
                    volume,
                    count,
                    volume > 0 ? notional / volume : NaN,
                ),
            )
        end
        i = j
    end
    return bars
end

"""
    tick_bars(trades, n; keep_partial = false) -> Vector{Bar}

Resample `trades` onto a transaction clock: one [`Bar`](@ref) per `n` prints.

The count is of prints as recorded, so whether odd lots and other
non-price-forming records count towards it is decided before the call — pass
`filter_price_forming(trades)` to exclude them. On a high-priced name that
choice changes the bar count by a factor of three, so it is not incidental.

Sampling by transaction count is the oldest of these clocks: Mandelbrot and Taylor (1967)
[doi:10.1287/opre.15.6.1057](https://doi.org/10.1287/opre.15.6.1057) proposed price changes
as a subordinated process running on transaction time, and Ané and Geman (2000)
[doi:10.1111/0022-1082.00286](https://doi.org/10.1111/0022-1082.00286) showed returns
sampled this way are close to normal where calendar-time returns are heavy-tailed.

Prints are ordered by exchange timestamp, not arrival. The print that
completes a bar belongs to it. Its timestamp is the bar's `close_ns`, so
consecutive bars satisfy `bars[i].close_ns <= bars[i+1].time_ns`. A trailing
incomplete bar is dropped unless `keep_partial`, since its threshold — and
therefore its comparability to the others — is not met.

    tick_bars(trades, 500)                      # one bar per 500 prints
    tick_bars(filter_price_forming(trades), 500)  # price-forming prints only
"""
function tick_bars(trades::AbstractVector{Trade}, n::Integer; keep_partial::Bool = false)
    n >= 1 || throw(ArgumentError("tick_bars needs n >= 1, got $n"))
    return _activity_bars(trades, n, _ -> 1.0; keep_partial)
end

"""
    volume_bars(trades, volume; keep_partial = false) -> Vector{Bar}

Resample `trades` onto a volume clock: one [`Bar`](@ref) per `volume` shares.

Clark (1973) [doi:10.2307/1913889](https://doi.org/10.2307/1913889) introduced volume as the
directing process for speculative prices, giving a finite-variance alternative to the
stable-Paretian account of heavy tails: the unconditional return distribution is
heavy-tailed because it mixes over a random volume clock, not because the underlying
increments are.

The caveat is mechanical: a share is not a fixed unit of economic activity
across time or across instruments. Share prices drift, splits reset the
scale, and a fixed share threshold therefore samples a different amount of
value in January than in December. [`dollar_bars`](@ref) is the usual remedy.
"""
function volume_bars(
    trades::AbstractVector{Trade},
    volume::Real;
    keep_partial::Bool = false,
)
    return _activity_bars(trades, volume, t -> t.size; keep_partial)
end

"""
    dollar_bars(trades, value; keep_partial = false) -> Vector{Bar}

Resample `trades` onto a traded-value clock: one [`Bar`](@ref) per `value` of
price times size.

This is the clock that survives changes of scale. It is invariant to splits,
and roughly invariant to price-level drift, so a threshold chosen on one
sample stays meaningful on another — the property neither [`tick_bars`](@ref)
nor [`volume_bars`](@ref) has, and the reason dollar bars are the usual
default for cross-sample work.

`value` is in the price's own units; no currency conversion is performed.
"""
function dollar_bars(trades::AbstractVector{Trade}, value::Real; keep_partial::Bool = false)
    return _activity_bars(trades, value, t -> t.price * t.size; keep_partial)
end


"""
    event_bars(trades) -> Vector{Bar}

Collapse the prints that share an exchange timestamp into one [`Bar`](@ref)
per instant: the matching event. A taker order filled against several
resting orders is reported as one print per fill, all at the matching
engine's single timestamp, so the print series is not the arrival process
of orders — on a liquid crypto pair nine prints in ten follow their
predecessor at a gap of exactly zero. The event series is: its `time_ns`
are the distinct instants, and the gap `bars[i+1].time_ns - bars[i].close_ns`
is the waiting time between events.

Each bar spans one instant (`time_ns == close_ns`) and carries the fills'
`open` (first), `close` (last), `high`, `low`, summed `volume`, their count
in `trade_count`, and the volume-weighted price in `vwap`. Prints are
ordered by exchange timestamp with ties kept in arrival order, so `open`
and `close` follow the fill sequence the venue reported.

Only identical timestamps are merged, so the result is as fine as the
capture's clock: a capture quantized to milliseconds (a Binance session
before 0.4.0, see `provenance.timestamp_unit` in its sidecar) merges
distinct events that fell in one millisecond. On a consolidated equity tape
identical nanosecond stamps across venues are rare, and an event is nearly
always one venue's sweep.

    events = event_bars(read_processed(cfg, "BTCUSDT"; from = d, to = d))
    waits_s = diff([e.time_ns for e in events]) ./ 1e9
"""
function event_bars(trades::AbstractVector{Trade})
    isempty(trades) && return Bar[]
    sym, ordered = _one_symbol_ordered(trades)
    bars = Bar[]
    i, n = 1, length(ordered)
    while i <= n
        first_print = ordered[i]
        stamp = first_print.time_ns
        high = low = first_print.price
        volume = 0.0
        notional = 0.0
        count = 0
        j = i
        while j <= n && ordered[j].time_ns == stamp
            t = ordered[j]
            high = max(high, t.price)
            low = min(low, t.price)
            volume += t.size
            notional += t.price * t.size
            count += 1
            j += 1
        end
        push!(
            bars,
            Bar(
                sym,
                stamp,
                stamp,
                0,
                first_print.price,
                high,
                low,
                ordered[j-1].price,
                volume,
                count,
                volume > 0 ? notional / volume : NaN,
            ),
        )
        i = j
    end
    return bars
end
