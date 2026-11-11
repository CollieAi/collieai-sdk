// File analysis (preview): the files client, attachments on CheckInputAsync,
// FileAnalysisUnavailableException and ModerationTimeoutException. Test names
// match the Python and Node suites.

using System.Net;
using System.Text.Json;
using Xunit;

namespace CollieAi.Client.Tests;

public class FilesTests
{
    private const string FileId = "file_0123456789abcdef0123456789abcdef";

    private static readonly string FileJson =
        "{\"id\":\"" + FileId + "\",\"object\":\"file\",\"bytes\":5,\"sha256\":\"" + new string('a', 64) + "\"," +
        "\"filename\":\"a.pdf\",\"declared_media_type\":\"application/pdf\",\"state\":\"uploaded\",\"retain\":true," +
        "\"created_at\":\"2026-11-10T12:00:00Z\",\"expires_at\":\"2026-11-10T13:00:00Z\"}";

    private static string Job(string extra = "", string status = "completed") =>
        "{\"job_id\":\"job_f\",\"status\":\"" + status + "\"," +
        "\"inbound_result\":{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]}" + extra + "}";

    private static Func<RecordedRequest, int, ResponseSpec> Jobs(string job) => (rec, _) =>
        rec.Method == HttpMethod.Post && rec.Path == "/v1/jobs"
            ? ResponseSpec.Ok("{\"job_id\":\"job_f\",\"status\":\"processing_inbound\"}")
            : ResponseSpec.Ok(job);

    // Records the content headers too: the API answers 411 without a length.
    private sealed class CapturingHandler : HttpMessageHandler
    {
        public long? ContentLength;
        public string? ContentType;
        public string Body = "";
        public string? Origin;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            ContentLength = request.Content?.Headers.ContentLength;
            ContentType = request.Content?.Headers.ContentType?.ToString();
            Body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(ct);
            Origin = request.Headers.TryGetValues("X-CollieAi-SDK-Origin", out var v) ? v.First() : null;
            return new HttpResponseMessage(HttpStatusCode.Created) { Content = new StringContent(FileJson) };
        }
    }

    // ---------------------------------------------------------------- files

    [Fact]
    public async Task Upload_sends_multipart_with_a_length_and_retain()
    {
        string path = Path.Combine(Path.GetTempPath(), $"collie-{Guid.NewGuid():N}.pdf");
        await File.WriteAllBytesAsync(path, "%PDF-"u8.ToArray());
        try
        {
            var handler = new CapturingHandler();
            var http = new HttpClient(handler) { BaseAddress = new Uri("https://test.collieai.io") };
            var client = new CollieClient(new CollieClientOptions { ApiKey = "clai_test", BaseUrl = http.BaseAddress },
                http, ownsHttp: true, logger: null, hooks: null);
            FileObject file = await client.Files.UploadAsync(path, retain: true);
            Assert.Equal(FileId, file.Id);
            Assert.True(file.Retain);
            Assert.StartsWith("multipart/form-data", handler.ContentType);
            Assert.NotNull(handler.ContentLength);
            Assert.Equal(handler.ContentLength, System.Text.Encoding.UTF8.GetByteCount(handler.Body));
            Assert.Contains("name=file; filename=" + Path.GetFileName(path), handler.Body);
            Assert.Contains("%PDF-", handler.Body);
            Assert.Contains("name=retain", handler.Body);
            Assert.Contains("true", handler.Body);
            Assert.Equal("files", handler.Origin);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Get_and_delete()
    {
        var (client, handler) = TestClient.Create((rec, _) =>
            rec.Method == HttpMethod.Delete ? new ResponseSpec(HttpStatusCode.NoContent) : ResponseSpec.Ok(FileJson));
        Assert.Equal("uploaded", (await client.Files.GetAsync(FileId)).State);
        await client.Files.DeleteAsync(FileId); // 204, no body
        Assert.Equal(new[] { (HttpMethod.Get, $"/v1/files/{FileId}"), (HttpMethod.Delete, $"/v1/files/{FileId}") },
            handler.Requests.Select(r => (r.Method, r.Path)));
    }

    [Fact]
    public async Task File_analysis_unavailable_on_its_code_only()
    {
        var (off, _) = TestClient.Create((_, _) =>
            ResponseSpec.Error(HttpStatusCode.UnprocessableEntity, "file_analysis_unavailable"));
        var ex = await Assert.ThrowsAsync<FileAnalysisUnavailableException>(() => off.Files.GetAsync(FileId));
        Assert.IsAssignableFrom<CollieApiException>(ex);
        Assert.Equal(422, ex.StatusCode);
        var (other, _) = TestClient.Create((_, _) => ResponseSpec.Error(HttpStatusCode.UnprocessableEntity, "invalid_retain"));
        var ex2 = await Assert.ThrowsAsync<CollieApiException>(() => other.Files.GetAsync(FileId));
        Assert.IsNotType<FileAnalysisUnavailableException>(ex2);
    }

    [Fact]
    public async Task A_malformed_file_object_is_a_typed_error()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok("{\"id\":\"x\"}"));
        var ex = await Assert.ThrowsAsync<CollieApiException>(() => client.Files.GetAsync("x"));
        Assert.Equal("invalid_response", ex.Code);
    }

    // ---------------------------------------------------------- attachments

    [Fact]
    public async Task Attachments_carry_the_deadline_computed_before_the_post()
    {
        var (client, handler) = TestClient.Create(Jobs(Job(",\"file_result\":{\"status\":\"rejected\",\"files\":[]}")),
            configure: o => o.Timeout = TimeSpan.FromSeconds(20));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest
        {
            Prompt = "summarize", Attachments = new[] { FileId }, TimeoutS = 10,
        });
        using var body = JsonDocument.Parse(handler.Requests[0].Body);
        var root = body.RootElement;
        Assert.Equal(FileId, root.GetProperty("attachments")[0].GetProperty("file_id").GetString());
        // The harness clock starts at the Unix epoch.
        Assert.Equal(DateTimeOffset.UnixEpoch, root.GetProperty("client_now").GetDateTimeOffset());
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(30), root.GetProperty("client_deadline_at").GetDateTimeOffset());
        Assert.Equal("summarize", root.GetProperty("message_input").GetString());
        Assert.Equal("rejected", result.FileResult!["status"].GetString());
    }

    [Fact]
    public async Task Without_attachments_the_body_is_unchanged()
    {
        var (client, handler) = TestClient.Create(Jobs(Job()));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.Equal("{\"message_input\":\"hi\",\"inbound_only\":true}", handler.Requests[0].Body);
        Assert.Null(result.FileResult);
    }

    [Fact]
    public async Task An_attachment_only_check()
    {
        var (client, handler) = TestClient.Create(Jobs(Job(",\"file_result\":{\"status\":\"rejected\"}")));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Attachments = new[] { FileId } });
        using var body = JsonDocument.Parse(handler.Requests[0].Body);
        Assert.False(body.RootElement.TryGetProperty("message_input", out _));
        Assert.True(body.RootElement.GetProperty("inbound_only").GetBoolean());
        Assert.Equal("", result.OriginalText);
        Assert.True(result.Allowed);
    }

    public static IEnumerable<object[]> Refusals() => new[]
    {
        new object[] { new InputModerationRequest() },
        new object[] { new InputModerationRequest { Attachments = Array.Empty<string>() } },
        new object[] { new InputModerationRequest { Attachments = new[] { "" } } },
    };

    [Theory]
    [MemberData(nameof(Refusals))]
    public async Task Refused_before_any_request(InputModerationRequest request)
    {
        var (client, handler) = TestClient.Create(Jobs(Job()));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => client.Moderation.CheckInputAsync(request));
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task A_missing_file_result_is_file_analysis_unavailable()
    {
        var (client, _) = TestClient.Create(Jobs(Job())); // a server that ignored the attachments
        var ex = await Assert.ThrowsAsync<FileAnalysisUnavailableException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", Attachments = new[] { FileId } }));
        Assert.Equal("file_result_missing", ex.Code);
    }

    [Fact]
    public async Task The_attachments_are_copied_when_the_call_starts()
    {
        // Emptying the caller's list mid-call must not switch off the check for
        // a server that ignored the attachments.
        var files = new List<string> { FileId };
        var jobs = Jobs(Job());
        var (client, _) = TestClient.Create((rec, i) =>
        {
            files.Clear();
            return jobs(rec, i);
        });
        await Assert.ThrowsAsync<FileAnalysisUnavailableException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", Attachments = files }));
    }

    [Fact]
    public async Task Attachments_refused_by_the_deployment()
    {
        var (client, _) = TestClient.Create((_, _) =>
            ResponseSpec.Error(HttpStatusCode.UnprocessableEntity, "file_analysis_unavailable"));
        await Assert.ThrowsAsync<FileAnalysisUnavailableException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", Attachments = new[] { FileId } }));
    }

    // -------------------------------------------------------------- timeout

    [Fact]
    public async Task Sdk_timeout_raises_moderation_timeout_subclass()
    {
        // The SDK's own poll budget running out throws
        // ModerationTimeoutException — a ModerationException, so existing catch
        // blocks keep catching it — with JobId, on input and output alike.
        Func<RecordedRequest, int, ResponseSpec> never(string status) => (_, _) =>
            ResponseSpec.Ok("{\"job_id\":\"job_slow\",\"status\":\"" + status + "\"}");
        var (input, _) = TestClient.Create(never("processing_inbound"), advanceClockOnDelay: true);
        ModerationException ex = await Assert.ThrowsAsync<ModerationTimeoutException>(() =>
            input.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi", TimeoutS = 5 }));
        Assert.Equal("job_slow", ((ModerationTimeoutException)ex).JobId);
        Assert.Contains("timed out", ex.Message);

        var (output, _) = TestClient.Create(never("processing_outbound"), advanceClockOnDelay: true);
        var outEx = await Assert.ThrowsAsync<ModerationTimeoutException>(() =>
            output.Moderation.CheckOutputAsync(new OutputModerationRequest { Response = "text", TimeoutS = 5 }));
        Assert.Equal("job_slow", outEx.JobId);
        Assert.IsAssignableFrom<ModerationException>(outEx);
    }
}
