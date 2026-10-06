// Coalesces provider deltas into chunks before submission, so customers don't
// POST once per token. Flush when the buffer reaches MaxProviderDeltas or
// MaxCharacters, when FlushInterval elapses since the first buffered delta, or
// when the provider ends. Empty windows are skipped. Mirrors the reference
// SDK's _batch_deltas.

using System.Runtime.CompilerServices;
using System.Text;

namespace CollieAi.Internal;

internal static class DeltaBatcher
{
    /// <summary>
    /// Batch an already-opened provider enumerator. The caller owns
    /// <paramref name="source"/>'s disposal (the batcher does not dispose it).
    /// <paramref name="delay"/> and <paramref name="nowSeconds"/> are injectable
    /// for deterministic tests. <paramref name="nowSeconds"/> is a MONOTONIC
    /// clock (seconds) — a wall clock made the flush window NTP-sensitive: a
    /// backward step turned a 50 ms flush into a long hang waiting for the next
    /// delta (v22).
    /// </summary>
    public static async IAsyncEnumerable<string> BatchAsync(
        IAsyncEnumerator<string> source,
        BatchingOptions options,
        Func<TimeSpan, CancellationToken, Task> delay,
        Func<double> nowSeconds,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var buffer = new StringBuilder();
        int deltas = 0;
        double windowStart = 0;
        Task<bool>? pending = null;
        double flushIntervalS = options.FlushInterval.TotalSeconds;

        try
        {
            while (true)
            {
                pending ??= source.MoveNextAsync().AsTask();

                if (buffer.Length > 0)
                {
                    TimeSpan remaining = TimeSpan.FromSeconds(flushIntervalS - (nowSeconds() - windowStart));
                    if (remaining <= TimeSpan.Zero)
                    {
                        yield return buffer.ToString();
                        buffer.Clear();
                        deltas = 0;
                        continue;
                    }

                    using var timerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    Task timer = delay(remaining, timerCts.Token);
                    Task winner = await Task.WhenAny(pending, timer).ConfigureAwait(false);
                    if (winner != pending)
                    {
                        // Flush timer elapsed before the next delta arrived.
                        yield return buffer.ToString();
                        buffer.Clear();
                        deltas = 0;
                        continue;
                    }
                    timerCts.Cancel(); // the delta won; stop the timer
                }

                bool moved = await pending.ConfigureAwait(false); // already completed (or waits when buffer empty)
                pending = null;
                if (!moved)
                    break; // provider ended

                string delta = source.Current;
                if (string.IsNullOrEmpty(delta))
                    continue; // skip empty deltas

                if (buffer.Length == 0)
                    windowStart = nowSeconds();
                buffer.Append(delta);
                deltas++;
                if (deltas >= options.MaxProviderDeltas || buffer.Length >= options.MaxCharacters)
                {
                    yield return buffer.ToString();
                    buffer.Clear();
                    deltas = 0;
                }
            }

            if (buffer.Length > 0)
                yield return buffer.ToString();
        }
        finally
        {
            // Observe an in-flight MoveNext so it doesn't surface as an unobserved
            // task fault while the caller tears the provider down.
            if (pending is not null)
            {
                try { await pending.ConfigureAwait(false); }
                catch { /* swallowed — teardown */ }
            }
        }
    }
}
