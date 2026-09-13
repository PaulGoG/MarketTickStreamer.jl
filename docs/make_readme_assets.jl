"""
Render the images the README and the manual show, into `docs/src/assets/`.

    julia --project=docs docs/make_readme_assets.jl <raw .jsonl> ...

Both assets come from the package's own code applied to a real capture, so
what a reader sees is what the tool produces rather than a mock-up:

  * `session_diagnostic.png` — `save_session_figures`, downscaled only.
  * `replay.gif` — a paced replay drawn frame by frame from the same
    `Channel{Trade}` a live session yields, which is the one claim a static
    figure cannot show.

The GIF is assembled with the system `ffmpeg`; without it the script skips
that asset and says so rather than failing. Committed PNG/GIF are kept small
deliberately: they are documentation, not data.
"""

using Pkg
Pkg.activate(@__DIR__; io = devnull)
Pkg.instantiate(; io = devnull)

using MarketTickStreamer
using CairoMakie
using Printf: @printf, @sprintf
const M = MarketTickStreamer

const ASSETS = joinpath(@__DIR__, "src", "assets")
const OKABE_BLUE = "#0072B2"
const OKABE_VERMILION = "#D55E00"
const OKABE_GREY = (:grey, 0.55)

# ---------------------------------------------------------------- static

"""Render the package's own per-session diagnostic and keep the PNG."""
function static_figure(raw_paths)
    mktempdir() do tmp
        files = save_session_figures(raw_paths, tmp; formats = ("png",))
        isempty(files) && error("no session figure produced from $(raw_paths)")
        # The busiest group is the representative one.
        src = argmax(filesize, files)
        dest = joinpath(ASSETS, "session_diagnostic.png")
        # save_session_figures exports at px_per_unit = 4, which is right for
        # print and four times wider than a README needs. Downscale rather than
        # re-render, so the committed image is the same figure.
        if Sys.which("ffmpeg") === nothing
            cp(src, dest; force = true)
        else
            run(`ffmpeg -y -v error -i $src -vf scale=1800:-1 $dest`)
        end
        @printf(
            "session_diagnostic.png  %.0f kB  (from %s)\n",
            filesize(dest) / 1024,
            basename(src)
        )
        return dest
    end
end

# ----------------------------------------------------------------- gif

"""
    replay_gif(raw_paths; n_frames = 96, fps = 12)

Draw a paced replay as it arrives. The consumer here is the ordinary one: it
reads a `Channel{Trade}` and cannot tell that the source is a recording.
"""
function replay_gif(raw_paths; n_frames::Int = 96, fps::Int = 12)
    Sys.which("ffmpeg") === nothing &&
        (@warn "ffmpeg not found — skipping replay.gif"; return nothing)

    # Drain once at maximum rate; the animation paces the *drawing*, which is
    # what a reader can see, rather than the wall-clock of the replay itself.
    trades = Trade[]
    for t in replay_source(collect(raw_paths); pace = "max")
        push!(trades, t)
    end
    isempty(trades) && error("replay produced no ticks")
    sort!(trades; by = t -> t.time_ns)
    sym = trades[1].symbol
    tz = M.tz"America/New_York"
    hours = [M._local_hour(t.time_ns; tz) for t in trades]
    prices = [t.price for t in trades]
    n = length(trades)
    cuts = round.(Int, range(max(2, n ÷ n_frames), n; length = n_frames))

    mktempdir() do tmp
        with_theme(merge(Theme(fontsize = 13, figure_padding = 10), tick_theme())) do
            for (k, c) in enumerate(cuts)
                fig = Figure(size = (760, 380))
                ax = Axis(
                    fig[1, 1];
                    xlabel = "Exchange time [HH:MM]",
                    ylabel = "Price [USD]",
                    xticks = M._hhmm_ticks(extrema(hours)...),
                )
                # Fixed limits: a rescaling axis reads as motion that is not
                # in the data.
                xlims!(ax, extrema(hours)...)
                ylims!(ax, extrema(prices)...)
                lines!(
                    ax,
                    M._decimate_minmax(hours[1:c], prices[1:c])...;
                    color = OKABE_BLUE,
                    linewidth = 1.4,
                )
                vlines!(ax, [hours[c]]; color = OKABE_VERMILION, linewidth = 1.0)
                text!(
                    ax,
                    0.015,
                    0.97;
                    text = @sprintf("%s   %s prints", sym, _grp(c)),
                    space = :relative,
                    align = (:left, :top),
                    fontsize = 12,
                )
                text!(
                    ax,
                    0.985,
                    0.03;
                    text = "replayed from a recording",
                    space = :relative,
                    align = (:right, :bottom),
                    fontsize = 10,
                    color = OKABE_GREY,
                )
                save(joinpath(tmp, @sprintf("f%04d.png", k)), fig; px_per_unit = 1)
            end
        end
        out = joinpath(ASSETS, "replay.gif")
        pal = joinpath(tmp, "pal.png")
        run(pipeline(`ffmpeg -y -v error -i $(joinpath(tmp, "f%04d.png"))
                      -vf palettegen=max_colors=64 $pal`))
        run(pipeline(`ffmpeg -y -v error -framerate $fps
                      -i $(joinpath(tmp, "f%04d.png")) -i $pal
                      -lavfi paletteuse=dither=bayer:bayer_scale=3 -loop 0 $out`))
        @printf(
            "replay.gif              %.0f kB  (%d frames at %d fps)\n",
            filesize(out) / 1024,
            length(cuts),
            fps
        )
        return out
    end
end

_grp(n::Integer) = replace(string(n), r"(?<=\d)(?=(\d{3})+$)" => " ")

function main(raw_paths)
    isempty(raw_paths) &&
        error("usage: julia --project=docs docs/make_readme_assets.jl <raw .jsonl> ...")
    mkpath(ASSETS)
    static_figure(raw_paths)
    replay_gif(raw_paths)
    return nothing
end

# Guarded with `if`, not `&&`: `@__FILE__` swallows a following `&&` and
# everything after it as macro arguments.
if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
