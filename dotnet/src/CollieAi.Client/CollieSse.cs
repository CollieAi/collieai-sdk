using System.Text.Json;

namespace CollieAi;

/// <summary>
/// Helpers for relaying safe stream events to a browser as SSE from your own
/// endpoint. Mirrors the SDK's wire-frame shapes so a JS <c>EventSource</c> sees
/// the same event names (<c>delta</c>, <c>blocked</c>, …).
/// </summary>
public static class CollieSse
{
    /// <summary>Re-encode a stream event as an SSE frame string ("event: …\ndata: …\n\n").</summary>
    public static string ToFrame(CollieStreamEvent ev)
    {
        ArgumentNullException.ThrowIfNull(ev);

        (string name, object payload) = ev switch
        {
            SafeDelta d => ("delta", (object)new { text = d.Text, sequence = d.Sequence }),
            Blocked b => ("blocked", (object)new { block_message = b.BlockMessage }),
            InputBlocked ib => ("input_blocked", WithContext(
                new Dictionary<string, object?> { ["block_message"] = ib.BlockMessage }, ib.BlockedBy, ib.Context)),
            Finished f => ("finished", WithContext(
                new Dictionary<string, object?> { ["finish_reason"] = f.FinishReason }, f.BlockedBy, f.Context)),
            StreamInterrupted si => ("interrupted", InterruptedFrame(si)),
            BufferedFallback bf => ("buffered_fallback", (object)new { reason = bf.Reason, recommended_client_behavior = bf.RecommendedClientBehavior }),
            _ => ("message", (object)new { }),
        };

        string json = JsonSerializer.Serialize(payload);
        return $"event: {name}\ndata: {json}\n\n";
    }

    /// <summary>Interrupted relay frame — carries the resume cursor
    /// (<c>last_event_id</c>) when present (v22), so a relay client that sees
    /// <c>resumable=true</c> actually has a point to resume from. Emitted as the
    /// SSE frame's own <c>id:</c> line too would require the caller to set it;
    /// here it rides the data payload for an application-level resume.</summary>
    private static object InterruptedFrame(StreamInterrupted si)
    {
        var fields = new Dictionary<string, object?>
        {
            ["reason"] = si.Reason,
            ["resumable"] = si.Resumable,
        };
        if (!string.IsNullOrEmpty(si.LastEventId))
            fields["last_event_id"] = si.LastEventId;
        return fields;
    }

    /// <summary>Add blocked_by + the snake_case context verdict to a relay frame
    /// so a browser sees the context block/pointer/rule + degraded markers. Keys
    /// omitted when absent.</summary>
    private static object WithContext(Dictionary<string, object?> fields, string? blockedBy, ContextModerationResult? context)
    {
        if (blockedBy is not null) fields["blocked_by"] = blockedBy;
        if (context is not null)
            fields["context"] = new
            {
                status = context.Status,
                blocked = context.Blocked,
                block_message = context.BlockMessage,
                triggering_pointer = context.TriggeringPointer,
                triggering_rule_id = context.TriggeringRuleId,
                triggering_rule_type = context.TriggeringRuleType,
                parse_degraded = context.ParseDegraded,
                limit_exceeded = context.LimitExceeded,
                inference_degraded = context.InferenceDegraded,
            };
        return fields;
    }
}
