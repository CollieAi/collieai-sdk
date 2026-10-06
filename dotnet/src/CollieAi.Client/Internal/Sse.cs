// Server-Sent Events parsing (consumer side) and a small encoder for relaying
// safe events to a browser from your own endpoint.

using System.Runtime.CompilerServices;
using System.Text.Json;

namespace CollieAi.Internal;

/// <summary>One parsed SSE event: the concatenated data plus optional id / type.</summary>
internal readonly record struct SseFrame(string? Id, string EventType, string Data);

internal static class Sse
{
    /// <summary>
    /// Parse a text reader of SSE lines into frames. Comment lines (": keepalive")
    /// are ignored; an event's <c>data</c> is the newline-join of its data lines.
    /// <paramref name="onActivity"/> fires on every received line — including
    /// keepalive comments — so the caller can reset an idle timer on traffic that
    /// never produces a frame.
    /// </summary>
    public static async IAsyncEnumerable<SseFrame> ParseAsync(
        TextReader reader, Action? onActivity = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        string? id = null;
        string? eventType = null;
        var data = new List<string>();

        string? line;
        while ((line = await reader.ReadLineAsync(ct).ConfigureAwait(false)) is not null)
        {
            onActivity?.Invoke(); // a line arrived (data or keepalive) — the stream is alive

            if (line.Length == 0) // event boundary
            {
                if (eventType is not null || data.Count > 0)
                    yield return new SseFrame(id, eventType ?? "message", string.Join("\n", data));
                id = null;
                eventType = null;
                data.Clear();
                continue;
            }
            if (line[0] == ':') // comment / keepalive
                continue;

            int colon = line.IndexOf(':');
            string field, value;
            if (colon < 0)
            {
                field = line;
                value = "";
            }
            else
            {
                field = line.Substring(0, colon);
                value = line.Substring(colon + 1);
                if (value.StartsWith(' '))
                    value = value.Substring(1);
            }

            switch (field)
            {
                case "event": eventType = value; break;
                case "data": data.Add(value); break;
                case "id": id = value; break;
            }
        }

        if (eventType is not null || data.Count > 0) // flush a final unterminated event
            yield return new SseFrame(id, eventType ?? "message", string.Join("\n", data));
    }
}
