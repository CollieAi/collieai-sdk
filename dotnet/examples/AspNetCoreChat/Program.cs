// Minimal ASP.NET Core example for the CollieAi .NET SDK.
//
// POST /chat streams ONLY CollieAi-released text. The provider stream is created
// inside RawStreamFactory, so it runs only after the input check passes — the
// unsafe path (forwarding raw model deltas) is never taken.
//
// Run:
//   dotnet run --project examples/AspNetCoreChat
//   curl -N -X POST localhost:5080/chat -H 'content-type: application/json' \
//        -d '{"message":"hello","conversationId":"c1","turnId":"t1"}'

using System.Runtime.CompilerServices;
using System.Text;
using CollieAi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCollieAi(options =>
{
    options.ApiKey = builder.Configuration["CollieAi:ApiKey"]
        ?? throw new InvalidOperationException("Set CollieAi:ApiKey (user-secrets or env COLLIEAI__APIKEY).");
    options.BaseUrl = new Uri(builder.Configuration["CollieAi:BaseUrl"] ?? "https://app.collieai.io");
    options.ProjectId = builder.Configuration["CollieAi:ProjectId"];
});

var app = builder.Build();

app.MapPost("/chat", (ChatRequest request, ICollieClient collie, CancellationToken ct) =>
    Results.Stream(async stream =>
    {
        try
        {
            await foreach (var ev in collie.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = request.Message,
                // Your LLM call goes here. It runs only after the input check passes.
                RawStreamFactory = token => CustomerLlm.StreamAsync(request.Message, token),
                ConversationId = request.ConversationId,
                CorrelationId = request.TurnId,
                CheckInput = true,
            }, ct))
            {
                switch (ev)
                {
                    case SafeDelta delta:
                        await WriteAsync(stream, delta.Text, ct);
                        break;
                    case InputBlocked inputBlocked:
                        await WriteAsync(stream, inputBlocked.BlockMessage ?? "Input blocked by policy.", ct);
                        return;
                    case Blocked blocked:
                        await WriteAsync(stream, blocked.BlockMessage ?? "Response blocked by policy.", ct);
                        return;
                }
            }
        }
        catch (ChunkStreamingUnsupportedException)
        {
            // The default path streams optimistically; a policy that can't stream
            // surfaces here. Switch the UX to "checking response…" and call
            // ProtectBufferedAsync instead (or set RequireStreaming = true to learn
            // this before generation, as BufferedFallbackRequiredException).
            await WriteAsync(stream, "This policy requires buffered response checking.", ct);
        }
        catch (BufferedFallbackRequiredException)
        {
            await WriteAsync(stream, "This policy requires buffered response checking.", ct);
        }
        catch (ChunkRetryExhaustedException)
        {
            await WriteAsync(stream, "The response could not be checked in time. Please try again.", ct);
        }
        catch (OperationCanceledException)
        {
            // Client disconnected — provider work was already cancelled by the SDK.
            // Note this does NOT inherit CollieException, so it never reaches the
            // fail-open decision below.
        }
        catch (CollieException ex)
        {
            // FAIL-CLOSED: nothing unverified is ever released. Failing OPEN when
            // CollieAi is unreachable is a deliberate risk decision, not a default
            // — see "Failure policy: fail-closed by default" in the .NET SDK docs
            // for how to do it safely. Two rules that section explains: catch only
            // CollieConnectionException / 5xx CollieApiException (this base type
            // also covers policy instructions like ChunkStreamingUnsupported), and
            // only before the first SafeDelta was written, or you re-emit output
            // the user has already seen.
            await WriteAsync(stream, $"Service error: {ex.Message}", ct);
        }
    }, "text/plain"));

app.Run();

static async Task WriteAsync(Stream stream, string text, CancellationToken ct)
{
    await stream.WriteAsync(Encoding.UTF8.GetBytes(text), ct);
    await stream.FlushAsync(ct);
}

/// <summary>Stand-in for the customer's own LLM. Replace with your provider call.</summary>
internal static class CustomerLlm
{
    public static async IAsyncEnumerable<string> StreamAsync(string prompt, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var word in $"You said: {prompt}. Here is a safe demo response.".Split(' '))
        {
            ct.ThrowIfCancellationRequested();
            await Task.Delay(20, ct);
            yield return word + " ";
        }
    }
}

internal sealed record ChatRequest(string Message, string? ConversationId, string? TurnId);
