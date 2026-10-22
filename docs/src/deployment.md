# Unattended capture under systemd

A capture host runs the live session as a systemd user service started by
a timer before the open. The package's own market-hours guards do the rest: the
session waits for the bell, stops at the close, and writes its provenance
whether it ends by the clock, by a signal, or by a crash.

## Configuration

Four keys of `config/config.toml` govern an unattended session:

```toml
[stream]
require_market_open = true   # check the venue clock before connecting
wait_for_open = true         # closed: sleep until the next open, then stream
stop_at_market_close = true  # schedule the stop at the session's close

[limits]
max_session_hours = 8.0      # from the moment streaming starts, not from launch
```

The wait precedes the session deadline: a process launched at 09:00 ET
waits for the 09:30 bell and then has `max_session_hours` to the hard stop,
so eight hours covers a regular session, its delayed-feed tail and a late
close. On a holiday the clock reports the next trading day's open and the
process sleeps to it; the timer's next firing finds the unit still active
and does nothing, which is the intended outcome. Keep
`[logging] log_to_file = true`: the journal holds the console stream and
`logs/` the session file, both named by the session id.

## Units

Two files under `~/.config/systemd/user/`. Paths assume a clone at
`~/MarketTickStreamer` and Julia installed by `juliaup`; adjust both.

`markettickstreamer-capture.service`:

```ini
[Unit]
Description=MarketTickStreamer live capture
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=%h/MarketTickStreamer
ExecStart=%h/.juliaup/bin/julia --threads=auto scripts/stream.jl config/config.toml
# The package handles SIGINT as a clean interrupt: the sidecar is finalized
# as "interrupted" and the raw files are flushed. SIGTERM would not be.
KillSignal=SIGINT
TimeoutStopSec=120
Restart=on-failure
RestartSec=120
Nice=5

[Install]
WantedBy=default.target
```

`markettickstreamer-capture.timer`:

```ini
[Unit]
Description=Start the MarketTickStreamer capture before the open

[Timer]
OnCalendar=Mon..Fri 09:00 America/New_York
AccuracySec=1min
Persistent=false

[Install]
WantedBy=timers.target
```

`Persistent=false` is deliberate: a firing missed while the host was down
should not start a capture at an arbitrary later hour; the next weekday's
firing will. `Restart=on-failure` restarts a session that ended in an
error (a fatal protocol error, exhausted reconnections) but not one that
ended by the clock, which exits cleanly.

## Enable

```bash
loginctl enable-linger "$USER"          # user units run without a login session
systemctl --user daemon-reload
systemctl --user enable --now markettickstreamer-capture.timer
systemctl --user list-timers            # next elapse
journalctl --user -u markettickstreamer-capture -f
```

A 24-hour venue needs no timer. Run the service alone with
`Restart=always`, `RestartSec=5` and `max_session_hours = 24.0`: the
deadline ends each day's session, the restart opens the next, and the gap
between them is the seconds the process takes to come up — recorded, as
every session boundary is, in the sidecars.

## Verifying a host

`systemd-analyze calendar "Mon..Fri 09:00 America/New_York"` prints the
next elapse in the host's zone and in UTC; `systemd-analyze --user verify
~/.config/systemd/user/markettickstreamer-capture.service` checks the
unit. Both units above were verified on systemd 259. The first run after
enabling is worth watching once through an open: the clock query, the wait,
the first prints, and the scheduled stop at the close appear in the journal
in that order.
