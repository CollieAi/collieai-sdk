using System.Net;
using CollieAi;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CollieAi.Client.Tests;

public class ClientCoreTests
{
    [Fact]
    public async Task AddCollieAi_resolves_a_typed_client()
    {
        var services = new ServiceCollection();
        services.AddCollieAi(o =>
        {
            o.ApiKey = "clai_x";
            o.BaseUrl = new Uri("https://app.collieai.io");
            o.ProjectId = "p";
        });

        // CollieClient is IAsyncDisposable-only (the SDK's `await using`
        // contract), so the container must be disposed asynchronously.
        await using var sp = services.BuildServiceProvider();
        var client = sp.GetRequiredService<ICollieClient>();

        Assert.NotNull(client);
        Assert.NotNull(client.Moderation);
        Assert.NotNull(client.Streaming);
    }

    [Fact]
    public void Constructor_requires_api_key()
    {
        Assert.Throws<ArgumentException>(() => new CollieClient(new CollieClientOptions { ApiKey = "" }));
    }

    [Fact]
    public async Task Metadata_with_control_chars_is_rejected_before_send()
    {
        var (client, handler) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.JobCreated()));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", ConversationId = "badid" }));

        Assert.Empty(handler.Requests); // never reached the wire
    }

    [Fact]
    public async Task Metadata_over_128_code_points_is_rejected_before_send()
    {
        var (client, handler) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.JobCreated()));
        var tooLong = new string('a', 129);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", CorrelationId = tooLong }));

        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task Empty_metadata_normalizes_to_unset_and_is_omitted()
    {
        var (client, handler) = TestClient.Create((rec, _) =>
            rec.Method == HttpMethod.Post ? ResponseSpec.Ok(Bodies.JobCreated()) : ResponseSpec.Ok(Bodies.InboundAllowed()));

        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", ConversationId = "", CorrelationId = null });

        var post = handler.Requests.First(r => r.Method == HttpMethod.Post && r.Path == "/v1/jobs");
        Assert.DoesNotContain("conversation_id", post.Body);
        Assert.DoesNotContain("correlation_id", post.Body);
        Assert.Contains("\"inbound_only\":true", post.Body);
    }

    [Fact]
    public async Task Per_call_project_mismatch_is_rejected()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.JobCreated()));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", ProjectId = "a-different-project" }));
    }
}
