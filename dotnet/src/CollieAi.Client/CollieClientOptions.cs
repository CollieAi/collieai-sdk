namespace CollieAi;

/// <summary>
/// Configuration for <see cref="CollieClient"/>. Bind from configuration in DI
/// (<c>AddCollieAi(...)</c>) or set directly for console/worker apps.
/// </summary>
public sealed class CollieClientOptions
{
    /// <summary>The CollieAi API key (<c>clai_…</c>). Required. Kept server-side only.</summary>
    public string ApiKey { get; set; } = "";

    /// <summary>API base URL. Defaults to the multi-tenant production host.</summary>
    public Uri BaseUrl { get; set; } = new Uri("https://app.collieai.io");

    /// <summary>
    /// The project the API key is scoped to. Job creation derives the project from
    /// the key, so this is used for consistency checks and forwarded to preflight.
    /// </summary>
    public string? ProjectId { get; set; }

    /// <summary>Per-request timeout for non-streaming calls. Does not bound long output streams.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Connection-pool cap per server. Raise for high-concurrency deployments.</summary>
    public int MaxConnectionsPerServer { get; set; } = 100;

    // --- Per-chunk retry policy ---

    /// <summary>Maximum submit attempts per chunk (same sequence + content each time).</summary>
    public int MaxRetriesPerChunk { get; set; } = 3;

    /// <summary>Total wall-clock retry ceiling per chunk.</summary>
    public TimeSpan RetryCeilingPerChunk { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Base exponential backoff.</summary>
    public TimeSpan BaseBackoff { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Maximum exponential backoff.</summary>
    public TimeSpan MaxBackoff { get; set; } = TimeSpan.FromSeconds(4);
}
