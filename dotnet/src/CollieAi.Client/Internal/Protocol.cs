// Wire protocol: internal request/response DTOs and JSON options.
//
// Wire fields are snake_case; DTO properties are PascalCase and rely on
// JsonNamingPolicy.SnakeCaseLower for the mapping. These DTOs are mapped to the
// clean public records (Models.cs) by the clients, so the public surface needs
// no serialization attributes. extra/unknown server fields are ignored by
// System.Text.Json, keeping older SDKs forward-compatible.

using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CollieAi.Internal;

internal static class Wire
{
    // The wire version identity is derived from the assembly, never hardcoded:
    // the package is packed from the csproj <Version>, so the emitted
    // User-Agent always equals the released version (the release gate
    // verifies it on the wire). Build metadata ("+<commit>") is stripped —
    // it is not part of the version identity.
    public static readonly string SdkVersion = ResolveSdkVersion();
    public static readonly string UserAgent = "CollieAi.Client/" + SdkVersion;

    private static string ResolveSdkVersion()
    {
        Assembly asm = typeof(Wire).Assembly;
        string? info = asm.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrEmpty(info))
        {
            int plus = info.IndexOf('+', StringComparison.Ordinal);
            return plus >= 0 ? info[..plus] : info;
        }
        return asm.GetName().Version?.ToString(3) ?? "0.0.0";
    }

    public static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true,
    };

    // SDK-origin header values — the public method that triggered a request.
    public const string OriginModerateInput = "moderate.input";
    public const string OriginModerateOutput = "moderate.output";
    public const string OriginPreflight = "streaming.preflight";
    public const string OriginProtectStream = "protect_stream";
    public const string OriginProtectBuffered = "protect_buffered";
    public const string OriginSession = "streaming.session";
}

// --- Request bodies ----------------------------------------------------------

internal sealed class JobCreateBody
{
    public string? MessageInput { get; set; }
    public string? MessageOutput { get; set; }
    public bool? InboundOnly { get; set; }
    public string? ConversationId { get; set; }
    public string? CorrelationId { get; set; }
    // Gate reference: job id of the
    // moderate.input call that already filtered this MessageInput. The server
    // verifies it and skips the second inbound pass; null is omitted (= the
    // job filters its own input, the pre-2.1 behavior).
    public string? InputJobId { get; set; }
    // Context-analysis input surface. object? so a
    // structured value (dict/list) serializes as JSON and a raw string as a JSON
    // string; null is omitted by the null-ignoring serializer (= unset).
    public object? Context { get; set; }
    public string? ContextFormat { get; set; }
}

internal sealed class ChunkBody
{
    public int Sequence { get; set; }
    public string Content { get; set; } = "";
    public bool IsFinal { get; set; }
    // Terminal chunk only; serialized as finish_reason and omitted when null
    // (WhenWritingNull). Persisted to the audit log (request_logs.finish_reason).
    public string? FinishReason { get; set; }
}

internal sealed class PreflightBody
{
    public string? ProjectId { get; set; }
    public string Direction { get; set; } = "outbound";
}

// --- Response DTOs -----------------------------------------------------------

internal sealed class JobCreateResponseDto
{
    public string? JobId { get; set; }
    public string? Status { get; set; }
}

internal sealed class JobStatusDto
{
    public string? Status { get; set; }
    public FilterResultDto? InboundResult { get; set; }
    public FilterResultDto? OutboundResult { get; set; }
    // Context analysis. Null on a job without it.
    public ContextResultDto? ContextResult { get; set; }
    public string? BlockedBy { get; set; }
    // Server pacing hint (Poll Backoff Contract). Deliberately a raw
    // JsonElement, NOT int?: a bad JSON type (true, "100") must be ignored by
    // the strict parse (PollPacer.ParseSuggestedPollMs), never fail the poll
    // with a deserialization error.
    public JsonElement? SuggestedPollMs { get; set; }
}

internal sealed class ContextResultDto
{
    public string? Status { get; set; }
    public bool Blocked { get; set; }
    public string? BlockMessage { get; set; }
    public string? TriggeringPointer { get; set; }
    public string? TriggeringRuleId { get; set; }
    public string? TriggeringRuleType { get; set; }
    public bool ParseDegraded { get; set; }
    public bool LimitExceeded { get; set; }
    public bool InferenceDegraded { get; set; }
}

internal sealed class FilterResultDto
{
    // Nullable to PRESERVE the wire's tri-state: true / false / absent are
    // distinct. ToResult resolves the fail-safe verdict (v23/v24) — an ABSENT
    // allowed is NOT an implicit allow; only an explicit allowed==true (and not
    // hard-blocked) is Allowed. Do not collapse absent to a default here.
    public bool? Allowed { get; set; }
    public bool Blocked { get; set; }
    public string? BlockMessage { get; set; }
    public string? FilteredContent { get; set; }
    public List<TriggeredRuleDto>? TriggeredRules { get; set; }
}

internal sealed class ChunkResponseDto
{
    // Nullable so a malformed 200 missing a required field is rejected as
    // invalid_response (see CollieClient.PostChunkAsync) rather than silently
    // deserializing to 0/false and advancing session state from a bogus body.
    public int? Sequence { get; set; }
    public bool? Accepted { get; set; }
    public List<EmitDto>? Emits { get; set; }
    public bool? Finished { get; set; }
}

internal sealed class EmitDto
{
    public string? Content { get; set; }
    public bool Blocked { get; set; }
    // Customer-facing message for blocked emits (F1); "" and null both mean
    // "no message" on the wire.
    public string? BlockMessage { get; set; }
    public bool Final { get; set; }
    public List<TriggeredRuleDto>? TriggeredRules { get; set; }
}

internal sealed class TriggeredRuleDto
{
    public string? RuleId { get; set; }
    public string? RuleName { get; set; }
    public string? RuleType { get; set; }
    public string? Decision { get; set; }
    public bool Monitoring { get; set; }
    public Dictionary<string, JsonElement>? MatchInfo { get; set; }
}

internal sealed class StreamTokenDto
{
    public string? StreamToken { get; set; }
    public int? ExpiresIn { get; set; }
}

internal sealed class StreamingCapabilityDto
{
    public string? Mode { get; set; }
    public string? RecommendedClientBehavior { get; set; }
    public string? ProjectId { get; set; }
    public string? StreamingMode { get; set; }
    public string? Reason { get; set; }
    public string? ReasonDetail { get; set; }
    public DateTimeOffset? ValidUntil { get; set; }
    public List<RuleCapabilityDto>? Rules { get; set; }
}

internal sealed class RuleCapabilityDto
{
    public string? RuleId { get; set; }
    public string? RuleName { get; set; }
    public string? RuleType { get; set; }
    public string? Decision { get; set; }
    public bool Monitoring { get; set; }
    public bool StreamingSupported { get; set; }
    public string? FallbackReason { get; set; }
    public string? ExecutionRole { get; set; }
}

// --- DTO -> public model mapping ---------------------------------------------

internal static class Mapping
{
    public static TriggeredRule ToModel(this TriggeredRuleDto dto) => new()
    {
        RuleId = dto.RuleId ?? "",
        RuleName = dto.RuleName ?? "",
        RuleType = dto.RuleType ?? "",
        Decision = dto.Decision ?? "",
        Monitoring = dto.Monitoring,
        MatchInfo = dto.MatchInfo,
    };

    // A JSON array can deserialize with null elements (e.g. `[null]`); mapping
    // those would throw a raw NullReferenceException. Reject them as the typed
    // invalid_response so a malformed list never escapes the error contract.
    private static T RequireElement<T>(T? element, string field) where T : class =>
        element ?? throw new CollieApiException(
            $"API response contained a null '{field}' array element", code: "invalid_response");

    public static IReadOnlyList<TriggeredRule> ToModels(this List<TriggeredRuleDto>? dtos)
    {
        if (dtos is null)
            return Array.Empty<TriggeredRule>();
        var result = new TriggeredRule[dtos.Count];
        for (int i = 0; i < dtos.Count; i++)
            result[i] = RequireElement(dtos[i], "triggered_rules").ToModel();
        return result;
    }

    public static IReadOnlyList<RuleCapability> ToModels(this List<RuleCapabilityDto>? dtos)
    {
        if (dtos is null)
            return Array.Empty<RuleCapability>();
        var result = new RuleCapability[dtos.Count];
        for (int i = 0; i < dtos.Count; i++)
            result[i] = RequireElement(dtos[i], "rules").ToModel();
        return result;
    }

    public static SafeEmit ToModel(this EmitDto dto) => new()
    {
        Text = dto.Content ?? "",
        Blocked = dto.Blocked,
        BlockMessage = string.IsNullOrEmpty(dto.BlockMessage) ? null : dto.BlockMessage,
        Final = dto.Final,
        TriggeredRules = dto.TriggeredRules.ToModels(),
    };

    public static ChunkResult ToModel(this ChunkResponseDto dto, string? jobId, string? requestId)
    {
        SafeEmit[] emits;
        if (dto.Emits is null)
        {
            emits = Array.Empty<SafeEmit>();
        }
        else
        {
            emits = new SafeEmit[dto.Emits.Count];
            for (int i = 0; i < dto.Emits.Count; i++)
                emits[i] = RequireElement(dto.Emits[i], "emits").ToModel();
        }

        return new ChunkResult
        {
            // Required fields are validated for presence before this is called.
            Sequence = dto.Sequence ?? 0,
            Accepted = dto.Accepted ?? false,
            Emits = emits,
            Finished = dto.Finished ?? false,
            JobId = jobId,
            RequestId = requestId,
        };
    }

    public static RuleCapability ToModel(this RuleCapabilityDto dto) => new()
    {
        RuleId = dto.RuleId ?? "",
        RuleName = dto.RuleName ?? "",
        RuleType = dto.RuleType ?? "",
        Decision = dto.Decision ?? "",
        Monitoring = dto.Monitoring,
        StreamingSupported = dto.StreamingSupported,
        FallbackReason = dto.FallbackReason,
        ExecutionRole = dto.ExecutionRole,
    };

    public static RecommendedClientBehavior ParseBehavior(string? wire) => wire switch
    {
        "stream" => RecommendedClientBehavior.Stream,
        "buffer_then_show" => RecommendedClientBehavior.BufferThenShow,
        // Unknown/"fail_fast"/null all map to fail-fast: refuse to render a token
        // stream we can't vouch for.
        _ => RecommendedClientBehavior.FailFast,
    };
}
