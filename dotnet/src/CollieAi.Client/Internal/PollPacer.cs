// Poll pacing per the Poll Backoff Contract.
//
// Normative spec: the Poll Backoff Contract (v27, 2026-07-23). One
// PollPacer instance per
// polling OPERATION — it is stateful (start time, deadline, ramp index) and
// must never be shared across concurrent jobs. The pacer owns timing only;
// the poll loops keep their flow-specific terminal checks and typed errors.
//
// Shape of the schedule:
//
// - warm phase (first 1.5 s): the caller's exact interval — no jitter, no
//   server hint. Zero-regression for fast jobs is unconditional.
// - ramp: doubling 100 → 1000 ms, floored at the caller's interval, with a
//   continuous jitter range whose lower bound is ALSO floored at the caller's
//   interval (the caller is never polled faster than they asked, post-jitter).
// - suggested_poll_ms server hints apply in the ramp only, strictly parsed.
// - 429 handling: any 429 is rate pressure — it ends the warm phase and the
//   wait is floored at PressureFloorS (Retry-After is a *minimum*, so waiting
//   longer is always compliant — "Retry-After: 0" must not tight-loop). The
//   jitter is upward-only, and a wait that could not complete strictly within
//   the remaining budget tells the caller to surface the typed 429 instead of
//   sleeping into a guaranteed poll-timeout.
// - every pending-poll sleep is bounded by the remaining wall-clock budget.
//
// All times are SECONDS from a monotonic source — the unit is normative; the
// client's default seam is Stopwatch.GetTimestamp() / Stopwatch.Frequency.

using System.Text.Json;

namespace CollieAi.Internal;

/// <summary>Per-operation poll pacing state machine (Poll Backoff Contract).</summary>
internal sealed class PollPacer
{
    public const double WarmS = 1.5;
    public static readonly double[] RampS = { 0.1, 0.2, 0.4, 0.8, 1.0 };
    public const double HardCapS = 5.0;
    public const double MinIntervalS = 0.01;
    public const int HintMinMs = 50;
    public const int HintMaxMs = 5000;
    // A 429 is rate pressure: never poll faster than this until the server
    // says otherwise, even when Retry-After says 0.
    public const double PressureFloorS = 1.0;
    // Upward-only jitter factor for Retry-After sleeps — the server's minimum
    // is never violated.
    public const double RetryAfterJitterFactor = 0.1;
    // Final-poll reserve (v10): a pending-path sleep that would otherwise
    // reach the deadline is shortened so ONE last GET is ISSUED with the
    // reserve remaining. The reserve guarantees the ISSUE, not the
    // completion: the response is accepted only if the full round trip
    // (network + body read + the loop's acceptance check) completes STRICTLY
    // within the reserve — at exactly 100 ms the strict boundary rejects it
    // (v12). v14 makes the reserve STATEFUL: the first pending response
    // LANDING inside the reserve polls again immediately — one preserved
    // final poll, no tail burst. v15: after that tail poll, polling
    // continues at the CALLER'S OWN interval until the deadline (sleeping
    // the whole remainder forfeited round trips that still fit the budget
    // under small custom timeouts). Together with the two densification
    // bounds, for round trips strictly inside the reserve any job
    // completing at least rtt + min(RESERVE, interval + rtt) before the
    // deadline succeeds under ANY schedule — the universal window
    // guarantee.
    public const double FinalPollReserveS = 0.1;
    // CancellationTokenSource.CancelAfter validates its delay against the
    // runtime timer bound; cap the budget explicitly (parity with the Node
    // SDK's MAX_TIMEOUT_S) instead of failing deep inside the loop.
    public const double MaxTimeoutS = 2147483.0; // 2^31−1 ms, floored to seconds

    private readonly double _interval;
    private readonly Func<double> _monotonic;
    private readonly Func<double> _rand;
    private readonly double _t0;
    private readonly double _capEff;
    private int _rampI;
    private bool _warmEnded;
    private bool _tailPollSpent;

    /// <summary>Absolute monotonic deadline (seconds), fixed at construction.</summary>
    public double Deadline { get; }

    /// <summary>
    /// Strict wire parse of <c>suggested_poll_ms</c>: a JSON integer within
    /// [50, 5000] ms → seconds; anything else → null (ignored). The strictness
    /// is normative — a lax parse would let <c>true</c> or <c>"100"</c> steer
    /// pacing. The DTO carries the raw <see cref="JsonElement"/> so a bad JSON
    /// type can never fail the poll itself.
    /// </summary>
    public static double? ParseSuggestedPollMs(JsonElement? value)
    {
        if (value is not { ValueKind: JsonValueKind.Number } element)
            return null;
        if (!element.TryGetInt32(out int ms))
            return null; // fractional or out-of-int-range numbers are ignored
        if (ms < HintMinMs || ms > HintMaxMs)
            return null;
        return ms / 1000.0;
    }

    /// <summary>Knob validation, callable BEFORE any side effect (job creation).</summary>
    public static void Validate(double intervalS, double timeoutS)
    {
        if (double.IsNaN(intervalS) || double.IsInfinity(intervalS) || intervalS < MinIntervalS)
            throw new ArgumentOutOfRangeException(
                nameof(intervalS), intervalS, $"poll interval must be a finite number >= {MinIntervalS}s");
        if (double.IsNaN(timeoutS) || double.IsInfinity(timeoutS) || timeoutS <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(timeoutS), timeoutS, "poll timeout must be a finite number > 0");
        if (timeoutS > MaxTimeoutS)
            throw new ArgumentOutOfRangeException(
                nameof(timeoutS), timeoutS,
                $"poll timeout must be <= {MaxTimeoutS}s (the runtime's 2^31-1 ms timer bound)");
    }

    public PollPacer(double intervalS, double timeoutS, Func<double> monotonic, Func<double> rand)
    {
        Validate(intervalS, timeoutS);
        _interval = intervalS;
        _monotonic = monotonic;
        _rand = rand;
        _t0 = monotonic();
        Deadline = _t0 + timeoutS;
        // An explicit caller interval above the hard cap is honored verbatim.
        _capEff = Math.Max(HardCapS, _interval);
    }

    // ------------------------------------------------------------------ state

    public double Remaining() => Deadline - _monotonic();

    public bool InWarm() => !_warmEnded && (_monotonic() - _t0) < WarmS;

    /// <summary>Force warm exit (used on any 429 — rate pressure).</summary>
    public void EndWarm() => _warmEnded = true;

    /// <summary>Advance the ramp exactly once per completed ramp-phase sleep —
    /// including sleeps stretched by a hint or a 429. Never resets.</summary>
    private void Bump() => _rampI = Math.Min(_rampI + 1, RampS.Length - 1);

    /// <summary>Bound by the remaining budget WITH the final-poll reserve
    /// (v10): a sleep that would otherwise reach the deadline is shortened so
    /// one last GET is ISSUED with the reserve remaining (accepted only if
    /// its round trip completes strictly within the reserve — v12). STATEFUL
    /// (v14): the first pending response that LANDS inside the reserve polls
    /// again immediately (the sleep-shortened path never ran, so the final
    /// poll is still unspent). After that tail poll, polling continues at
    /// the caller's own interval until the deadline (v15) — the
    /// caller-sanctioned density is not a burst, and sleeping the whole
    /// remainder forfeited round trips that still fit the budget.</summary>
    private double Bound(double sleepS)
    {
        double remaining = Remaining();
        double tail = Math.Max(0.0, remaining - FinalPollReserveS);
        if (sleepS <= tail)
            return Math.Max(0.0, sleepS);
        if (remaining > FinalPollReserveS)
        {
            _tailPollSpent = true;
            return tail;
        }
        if (!_tailPollSpent)
        {
            _tailPollSpent = true;
            return 0.0;
        }
        return Math.Min(_interval, Math.Max(0.0, remaining));
    }

    // ----------------------------------------------------------------- sleeps

    /// <summary>Sleep (seconds) before the next status GET, after a pending 2xx response.</summary>
    public double NextSleep(JsonElement? suggestedPollMs = null)
    {
        if (InWarm())
        {
            // Literal warm phase: exact caller interval — no jitter, no hint.
            return Bound(_interval);
        }
        double baseS = Math.Max(RampS[_rampI], _interval);
        Bump();
        double? hintS = ParseSuggestedPollMs(suggestedPollMs);
        double target = Math.Min(hintS is { } h ? Math.Max(baseS, h) : baseS, _capEff);
        // Deadline-aware densification (v14): the target never exceeds HALF
        // the remaining budget (floored at the caller's interval) — as the
        // deadline nears, the schedule geometrically approaches the plain
        // schedule's landing density instead of gambling a job completing
        // just inside the budget on one sparse final landing.
        target = Math.Min(target, Math.Max(Remaining() / 2.0, _interval));
        // Ramp-in bound (v15, symmetric to densification): the target never
        // exceeds HALF the operation's own elapsed time either — a slow
        // host median used to jump a young job straight from the warm phase
        // into 5 s sleeps (a 1.6 s job cost +4.4 s). The geometric ramp-in
        // caps hint overhead at ~60 % of the job's own duration while
        // converging to the full hint within a few sleeps on long jobs.
        target = Math.Min(target, Math.Max((_monotonic() - _t0) / 2.0, _interval));
        // Continuous jitter range: the lower bound is floored at the caller's
        // interval (post-jitter floor), the upper bound at capEff — no point
        // mass at the cap. A degenerate lo >= hi means "intentionally exact"
        // (caller-pinned interval).
        double lo = Math.Max(0.8 * target, _interval);
        double hi = Math.Min(1.2 * target, _capEff);
        double raw = hi <= lo ? lo : lo + ((hi - lo) * _rand());
        return Bound(raw);
    }

    /// <summary>
    /// 429 handling. Returns the sleep (seconds) to take, or null when the
    /// caller must surface the typed 429 — the required wait could not
    /// complete strictly within the remaining budget, and sleeping into a
    /// guaranteed poll-timeout would only mask the rate limit.
    ///
    /// Any 429 is rate pressure, so the wait is floored at
    /// <see cref="PressureFloorS"/> even for <c>Retry-After: 0</c> — the
    /// header is a *minimum*, waiting longer is always compliant. It is ALSO
    /// floored at the caller's interval (v18): a 429 is rate pressure and
    /// must never make the caller poll FASTER than they asked — with a 10 s
    /// interval, <c>Retry-After: 0</c> used to collapse to the 1 s pressure
    /// floor, a 10x speed-up in response to backpressure. The upward-only
    /// jitter is a courtesy: when the jittered value would reach the
    /// remaining budget (leaving no room for one more GET), it falls back to
    /// the un-jittered floor rather than consuming the final poll.
    /// </summary>
    public double? SleepFor429(double? retryAfterS)
    {
        EndWarm();
        Bump();
        if (retryAfterS is { } ra && ra >= 0)
        {
            double floor = Math.Max(Math.Max(ra, PressureFloorS), _interval);
            // Feasibility includes the final-poll reserve (v11): the wait AND
            // the final GET's issue reserve must both fit the budget — a
            // wait leaving less than the reserve would launch a GET doomed
            // by the strict acceptance boundary.
            if (floor >= Remaining() - FinalPollReserveS)
                return null;
            double jittered = floor * (1.0 + (RetryAfterJitterFactor * _rand()));
            // Re-decided against ONE fresh reading AFTER the jitter draw
            // (TOCTOU).
            double budget = Remaining() - FinalPollReserveS;
            if (jittered < budget)
                return jittered;
            return floor < budget ? floor : null;
        }
        // No usable header: poll no faster than the pressure floor.
        double baseS = Math.Max(RampS[_rampI], _interval);
        double target = Math.Min(Math.Max(baseS, PressureFloorS), _capEff);
        return target < Remaining() - FinalPollReserveS ? target : null;
    }
}
