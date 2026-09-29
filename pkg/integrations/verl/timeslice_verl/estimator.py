"""expected_idle estimator: how long the trainer will leave the GPU idle after a Yield.

Pure functions, no verl/grpc imports, so the verl hooks, trainer stand-ins
and tests all share one rule. The Go mirror is
``pkg/integrations/verl/expectedidle``; both are checked against
``tests/testdata/expected_idle_parity.json``.

Rule, per yield point: the hint is an exponentially weighted moving average
(ALPHA = 0.5) of the measured gaps between this point's Yield and the next
Acquire. The first observed gap is taken as is. A point with no observed gap
yet gives no hint (None). Negative and NaN gaps are ignored.

API (fixed; stand-ins import it):

  new() -> state
  next(state, point) -> float | None     seconds; None = no hint
  observe(state, point, gap_s) -> None

The hint source is chosen with ``TIMESLICE_EXPECTED_IDLE`` (see parse_mode):
``off`` (default: no hint, today's behaviour), ``auto`` (this estimator) or a
fixed number of seconds.
"""

import math

ALPHA = 0.5

ENV_MODE = "TIMESLICE_EXPECTED_IDLE"
MODE_OFF = "off"
MODE_AUTO = "auto"
MODE_FIXED = "fixed"


def new() -> dict:
    """Return an empty estimator state (estimate per yield point)."""
    return {}


def next(state: dict, point: str) -> float | None:  # noqa: A001 - fixed API name
    """Hint in seconds for the next Yield at `point`, or None when no gap was observed there yet."""
    return state.get(point)


def observe(state: dict, point: str, gap_s: float) -> None:
    """Record a measured gap (seconds) between a Yield at `point` and the next Acquire."""
    gap = float(gap_s)
    if gap < 0 or math.isnan(gap):
        return
    prev = state.get(point)
    state[point] = gap if prev is None else ALPHA * gap + (1 - ALPHA) * prev


def parse_mode(raw: str | None) -> tuple[str, float]:
    """Parse a TIMESLICE_EXPECTED_IDLE value into (mode, seconds).

    "off" or empty -> (MODE_OFF, 0); "auto" -> (MODE_AUTO, 0); a non-negative
    number -> (MODE_FIXED, seconds). Case and surrounding spaces are ignored.
    Raises ValueError on anything else.
    """
    val = (raw or "").strip().lower()
    if val in ("", MODE_OFF):
        return MODE_OFF, 0.0
    if val == MODE_AUTO:
        return MODE_AUTO, 0.0
    try:
        secs = float(val)
    except ValueError:
        secs = math.nan
    if math.isnan(secs) or math.isinf(secs) or secs < 0:
        raise ValueError(f"invalid {ENV_MODE} {raw!r}: want off, auto or a non-negative number of seconds")
    return MODE_FIXED, secs
