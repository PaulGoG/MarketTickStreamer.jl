"""
Render the images the README and the manual show, into `docs/src/assets/`.

    julia docs/make_readme_assets.jl <raw .jsonl> ...

Both assets come from the package's own code applied to a real capture, so
what a reader sees is what the tool produces rather than a mock-up:

  * `session_diagnostic.png` — `save_session_figures`, downscaled only.
  * `replay.gif` — a paced replay drawn frame by frame from the same
    `Channel{Trade}` a live session yields, which a static figure cannot
    show.

The GIF is assembled with the system `ffmpeg`; without it the script skips
that asset and says so rather than failing. Committed PNG/GIF are kept small:
they are documentation, not data.
"""

include(joinpath(@__DIR__, "activate.jl"))

using MarketTickStreamer
using CairoMakie
using Printf: @printf, @sprintf
const M = MarketTickStreamer
# The figure extension, loaded with CairoMakie above.
const E = Base.get_extension(M, :MarketTickStreamerMakieExt)

const ASSETS = joinpath(@__DIR__, "src", "assets")
const OKABE_BLUE = "#0072B2"
const OKABE_VERMILION = "#D55E00"
const OKABE_GREY = (:grey, 0.55)
const GIF_WIDTH = 760       # px; frames are rendered larger and scaled down

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

"""
    replay_gif(raw_paths; n_frames = 120, fps = 12, hold_s = 2.0)

Draw a paced replay as it arrives. The consumer here is the ordinary one: it
reads a `Channel{Trade}` and cannot tell that the source is a recording.

Frames are spaced uniformly in exchange time, as a paced replay delivers the
ticks: the cursor sweeps at constant speed and the print counter climbs with
the trading rate. Spacing them by print count instead crawls through the busy
regular session and crosses the sparse extended hours in a frame or two. The
completed path is held for `hold_s` seconds before the loop restarts.
"""
function replay_gif(raw_paths; n_frames::Int = 120, fps::Int = 12, hold_s::Real = 2.0)
    Sys.which("ffmpeg") === nothing &&
        (@warn "ffmpeg not found; skipping replay.gif"; return nothing)

    # Drain once at maximum rate; the animation paces the *drawing*, which is
    # what a reader can see, rather than the wall-clock of the replay itself.
    trades = Trade[]
    for t in replay_source(collect(raw_paths); pace = :max)
        push!(trades, t)
    end
    isempty(trades) && error("replay produced no ticks")
    sort!(trades; by = t -> t.time_ns)
    sym = trades[1].symbol
    tz = M.tz"America/New_York"
    # The counter counts every print replayed; the path is drawn from the
    # price-forming ones, as in `session_figure`.
    hours_all = [M._local_hour(t.time_ns; tz) for t in trades]
    keep = [price_forming(t) for t in trades]
    any(keep) || fill!(keep, true)
    hours = hours_all[keep]
    prices = [t.price for t in trades[keep]]
    t₀, t₁ = extrema(hours)
    frame_hours = range(t₀, t₁; length = n_frames + 1)[2:end]

    mktempdir() do tmp
        # Frames are rendered at the standard canvas and type sizes and scaled
        # down when the GIF is assembled, so the proportions are those of the
        # static figures.
        with_theme(tick_theme()) do
            for (k, tₖ) in enumerate(frame_hours)
                c = searchsortedlast(hours, tₖ)
                fig = Figure(size = (1200, 600))
                ax = Axis(
                    fig[1, 1];
                    xlabel = "Exchange time [HH:MM]",
                    ylabel = "Price [USD]",
                    xticks = M._hhmm_ticks(t₀, t₁),
                )
                # Fixed limits: a rescaling axis reads as motion that is not
                # in the data.
                xlims!(ax, t₀, t₁)
                ylims!(ax, extrema(prices)...)
                c >= 2 && lines!(
                    ax,
                    M._decimate_minmax(hours[1:c], prices[1:c])...;
                    color = OKABE_BLUE,
                )
                # The cursor marks the replay clock; the completed path has none.
                k < n_frames && vlines!(
                    ax,
                    [tₖ];
                    color = OKABE_VERMILION,
                    linewidth = 1.5,
                    linestyle = :dash,
                )
                text!(
                    ax,
                    0.015,
                    0.97;
                    text = @sprintf(
                        "%s   %s prints",
                        sym,
                        _grp(searchsortedlast(hours_all, tₖ))
                    ),
                    space = :relative,
                    align = (:left, :top),
                    fontsize = E.ANNOTATION_FONTSIZE,
                )
                text!(
                    ax,
                    0.985,
                    0.03;
                    text = "replayed from a recording",
                    space = :relative,
                    align = (:right, :bottom),
                    fontsize = E.ANNOTATION_FONTSIZE,
                    color = OKABE_GREY,
                )
                save(joinpath(tmp, @sprintf("f%04d.png", k)), fig; px_per_unit = 1)
            end
        end
        out = joinpath(ASSETS, "replay.gif")
        pal = joinpath(tmp, "pal.png")
        run(
            pipeline(
                `ffmpeg -y -v error -i $(joinpath(tmp, "f%04d.png"))
                      -vf scale=$(GIF_WIDTH):-1:flags=lanczos,palettegen=max_colors=64 $pal`,
            ),
        )
        run(
            pipeline(
                `ffmpeg -y -v error -framerate $fps
                      -i $(joinpath(tmp, "f%04d.png")) -i $pal
                      -lavfi "[0:v]tpad=stop_mode=clone:stop_duration=$(hold_s),scale=$(GIF_WIDTH):-1:flags=lanczos[s];[s][1:v]paletteuse=dither=bayer:bayer_scale=3"
                      -loop 0 $out`,
            ),
        )
        @printf(
            "replay.gif              %.0f kB  (%d frames at %d fps, last held %.1f s)\n",
            filesize(out) / 1024,
            n_frames,
            fps,
            hold_s
        )
        return out
    end
end

_grp(n::Integer) = replace(string(n), r"(?<=\d)(?=(\d{3})+$)" => " ")

function main(raw_paths)
    isempty(raw_paths) && error("usage: julia docs/make_readme_assets.jl <raw .jsonl> ...")
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
