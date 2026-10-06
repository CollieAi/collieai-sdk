// Mutation-kill tests (#26 .NET streaming target): each test exists because
// a specific surviving/no-coverage mutant in StreamingClient.cs proved the
// class unpinned (the maintainers' mutation-testing register, ".NET streaming").
using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

public class MutationKillTests
{
    // -- shared chunk-capable route -----------------------------------------

    private sealed class ChunkRoute
    {
        public readonly List<RecordedRequest> JobPosts = new();
        public readonly List<RecordedRequest> ChunkPosts = new();
        public bool BlockOnFirstChunk;
        public bool FinishOnFirstChunk;
        public bool EmitOnFinal = true;

        public ResponseSpec Handle(RecordedRequest rec, int _)
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            {
                JobPosts.Add(rec);
                if (rec.Body.Contains("\"inbound_only\":true"))
                    return ResponseSpec.Ok(Bodies.JobCreated("job_in"));
                return ResponseSpec.Ok(Bodies.JobCreated("job_sess"));
            }
            if (rec.Path == "/v1/jobs/job_in" && rec.Method == HttpMethod.Get)
                return ResponseSpec.Ok(Bodies.InboundAllowed());
            if (rec.Path.EndsWith("/chunks"))
            {
                ChunkPosts.Add(rec);
                using var doc = System.Text.Json.JsonDocument.Parse(rec.Body);
                int seq = doc.RootElement.GetProperty("sequence").GetInt32();
                bool isFinal = doc.RootElement.GetProperty("is_final").GetBoolean();
                string content = doc.RootElement.TryGetProperty("content", out var c)
                    ? (c.GetString() ?? "") : "";
                if (BlockOnFirstChunk && content.Length > 0)
                    return ResponseSpec.Ok(Bodies.ChunkBlocked(seq));
                if (FinishOnFirstChunk && content.Length > 0)
                    return ResponseSpec.Ok(
                        "{\"sequence\":" + seq + ",\"accepted\":true,\"emits\":[{\"content\":\""
                        + content + "\",\"blocked\":false,\"final\":true,\"triggered_rules\":[]}],\"finished\":true}");
                if (isFinal && EmitOnFinal)
                    return ResponseSpec.Ok(
                        "{\"sequence\":" + seq + ",\"accepted\":true,\"emits\":[{\"content\":\"tail\",\"blocked\":false,\"final\":true,\"triggered_rules\":[]}],\"finished\":true}");
                return ResponseSpec.Ok(isFinal ? Bodies.ChunkFinal(seq) : Bodies.ChunkEmit(seq, content));
            }
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        }
    }

    private static async IAsyncEnumerable<string> Endless(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        int i = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return $"d{i++}";
        }
    }

    private static async Task<List<CollieStreamEvent>> CollectAsync(
        IAsyncEnumerable<CollieStreamEvent> stream, CancellationTokenSource? cts = null)
    {
        // WaitAsync bound: a break→continue regression must be a
        // DETERMINISTIC xUnit failure, not a hang only a mutation
        // harness's timeout would catch. On timeout the SOURCE is
        // cancelled and awaited, so a failing test does not leave an
        // endless provider running in the testhost (round-5 review).
        var task = Task.Run(async () =>
        {
            var list = new List<CollieStreamEvent>();
            await foreach (var ev in stream) list.Add(ev);
            return list;
        });
        try
        {
            return await task.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (TimeoutException)
        {
            cts?.Cancel();
            try { await task.WaitAsync(TimeSpan.FromSeconds(5)); } catch { /* cleanup best-effort */ }
            throw;
        }
    }

    // -- terminal loop contracts (survivors L348/359/369/370/374) -------------

    [Fact]
    public async Task Midstream_block_is_one_submission_and_no_final_flush()
    {
        var route = new ChunkRoute { BlockOnFirstChunk = true };
        var (client, _) = TestClient.Create(route.Handle);

        using var cts = new CancellationTokenSource();
        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "hi",
            RawStreamFactory = ct => Endless(ct),
        }, cts.Token), cts);

        Assert.Single(events);
        Assert.IsType<Blocked>(events[0]);
        Assert.Single(route.ChunkPosts); // the block broke the submit loop
        Assert.DoesNotContain(route.ChunkPosts, r => r.Body.Contains("\"is_final\":true"));
    }

    [Fact]
    public async Task Server_finish_is_one_submission_and_no_final_flush()
    {
        var route = new ChunkRoute { FinishOnFirstChunk = true };
        var (client, _) = TestClient.Create(route.Handle);

        using var cts = new CancellationTokenSource();
        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "hi",
            RawStreamFactory = ct => Endless(ct),
        }, cts.Token), cts);

        Assert.IsType<Finished>(events[^1]);
        Assert.Single(route.ChunkPosts);
        Assert.DoesNotContain(route.ChunkPosts, r => r.Body.Contains("\"is_final\":true"));
        // exactly ONE terminal for the whole turn
        Assert.Equal(1, events.Count(e => e is Finished or Blocked or InputBlocked));
    }

    // -- final flush wire + the flush's own emits (L376/391 + NoCov 379-381) --

    [Fact]
    public async Task Final_flush_carries_stop_and_its_emits_are_yielded()
    {
        var route = new ChunkRoute { EmitOnFinal = true };
        var (client, _) = TestClient.Create(route.Handle);

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "hi",
            RawStreamFactory = _ => TestSupportStreams.Of("a", "b"),
        }));

        var finals = route.ChunkPosts.Where(r => r.Body.Contains("\"is_final\":true")).ToList();
        var final = Assert.Single(finals);
        Assert.Contains("\"finish_reason\":\"stop\"", final.Body);
        Assert.DoesNotContain(
            route.ChunkPosts.Where(r => !r.Body.Contains("\"is_final\":true")),
            r => r.Body.Contains("finish_reason"));
        // The final flush's OWN emit reached the caller, before Finished.
        Assert.Contains(events, e => e is SafeDelta d && d.Text == "tail");
        Assert.IsType<Finished>(events[^1]);
        Assert.Equal(1, events.Count(e => e is Finished or Blocked or InputBlocked));
    }

    // -- buffered text accumulation (survivors L517-521) ----------------------

    [Fact]
    public async Task Buffered_create_carries_the_full_accumulated_text()
    {
        var posts = new List<RecordedRequest>();
        Func<RecordedRequest, int, ResponseSpec> route = (rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            {
                posts.Add(rec);
                if (rec.Body.Contains("\"inbound_only\":true"))
                    return ResponseSpec.Ok(Bodies.JobCreated("job_in"));
                return ResponseSpec.Ok(Bodies.JobCreated("job_buf"));
            }
            if (rec.Path == "/v1/jobs/job_in") return ResponseSpec.Ok(Bodies.InboundAllowed());
            if (rec.Path == "/v1/jobs/job_buf") return ResponseSpec.Ok(Bodies.OutboundCompleted("clean"));
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        };
        var (client, _) = TestClient.Create(route);

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "hi",
            RawStreamFactory = _ => TestSupportStreams.Of("part one, ", "part two."),
        });

        var bufferedCreate = posts.Single(r => !r.Body.Contains("\"inbound_only\":true"));
        Assert.Contains("part one, part two.", bufferedCreate.Body);
        Assert.False(result.Blocked);
    }

    // -- the buffered edge guard (survivor L461 + NoCov L462) ------------------

    [Fact]
    public async Task Not_allowed_external_input_result_rejected_at_the_edge_buffered()
    {
        var (client, _) = TestClient.Create((rec, _) =>
            ResponseSpec.Error(HttpStatusCode.NotFound, "not_found"));

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
            {
                Input = "hi",
                RawStreamFactory = _ => TestSupportStreams.Of("never"),
                InputResult = new InputModerationResult
                {
                    Allowed = false, Blocked = false, OriginalText = "hi",
                },
            }));
        Assert.Contains("be allowed", ex.Message);
    }

    // -- buffered create without job_id (NoCov L537) ---------------------------

    [Fact]
    public async Task Buffered_create_without_job_id_is_invalid_response()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            {
                if (rec.Body.Contains("\"inbound_only\":true"))
                    return ResponseSpec.Ok(Bodies.JobCreated("job_in"));
                return ResponseSpec.Ok("{}"); // no job_id
            }
            if (rec.Path == "/v1/jobs/job_in") return ResponseSpec.Ok(Bodies.InboundAllowed());
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        });

        var ex = await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
            {
                Input = "hi",
                RawStreamFactory = _ => TestSupportStreams.Of("a"),
            }));
        Assert.Equal("invalid_response", ex.Code);
    }

    // -- buffered disposes the provider (survivors L526-528) ------------------

    private sealed class TrackedStream : IAsyncEnumerable<string>
    {
        // A MANUAL enumerator: the flag flips ONLY in DisposeAsync, so
        // this pins the explicit DisposeAsync call — a compiler-generated
        // iterator's finally also runs on natural completion and proves
        // nothing about the dispose statement (round-5 review).
        public bool DisposeCalled;

        public IAsyncEnumerator<string> GetAsyncEnumerator(CancellationToken ct = default)
            => new Enumerator(this);

        private sealed class Enumerator(TrackedStream owner) : IAsyncEnumerator<string>
        {
            private int _i;
            public string Current { get; private set; } = "";
            public ValueTask<bool> MoveNextAsync()
            {
                if (_i >= 2) return ValueTask.FromResult(false);
                Current = _i++ == 0 ? "a" : "b";
                return ValueTask.FromResult(true);
            }
            public ValueTask DisposeAsync()
            {
                owner.DisposeCalled = true;
                return ValueTask.CompletedTask;
            }
        }
    }

    [Fact]
    public async Task Buffered_disposes_the_provider_enumerator()
    {
        var route = new ChunkRoute();
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated(
                    rec.Body.Contains("\"inbound_only\":true") ? "job_in" : "job_buf"));
            if (rec.Path == "/v1/jobs/job_in") return ResponseSpec.Ok(Bodies.InboundAllowed());
            if (rec.Path == "/v1/jobs/job_buf") return ResponseSpec.Ok(Bodies.OutboundCompleted("x"));
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        });
        var provider = new TrackedStream();

        await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "hi",
            RawStreamFactory = _ => provider,
        });
        Assert.True(provider.DisposeCalled);
    }

    // -- argument validation removals (survivors L58/142/451/661) -------------

    [Fact]
    public async Task Null_requests_throw_typed_argument_null()
    {
        var (client, _) = TestClient.Create((rec, _) =>
            ResponseSpec.Error(HttpStatusCode.NotFound, "not_found"));
        Assert.Throws<ArgumentNullException>(() => client.Streaming.ProtectStreamAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.Streaming.PreflightAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.Streaming.ProtectBufferedAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.Streaming.CreateSessionAsync(null!));
    }

    // -- preflight cache reuse under RequireStreaming (survivor L191) ----------

    [Fact]
    public async Task Require_streaming_reuses_the_cached_preflight()
    {
        int preflights = 0;
        var route = new ChunkRoute();
        var (client, _) = TestClient.Create((rec, i) =>
        {
            if (rec.Path == "/v1/streaming/preflight")
            {
                preflights++;
                return ResponseSpec.Ok(Bodies.PreflightStreaming());
            }
            return route.Handle(rec, i);
        });

        for (int i = 0; i < 2; i++)
        {
            await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "hi",
                RawStreamFactory = _ => TestSupportStreams.Of("a"),
                RequireStreaming = true,
            }));
        }
        Assert.Equal(1, preflights);
    }
}

public class MutationKillTests2
{
    // -- blocked:true, finished:false — malformed but parseable (L359) --------

    [Fact]
    public async Task Blocked_without_finished_still_terminates_after_one_submission()
    {
        // The DTO accepts blocked:true, finished:false — no parser
        // cross-field invariant exists, so the server contract "blocked
        // implies finished" must NOT be what stops the loop. The blocked
        // branch's own break is load-bearing: one submission, one Blocked
        // terminal, even against a malformed-but-parseable response.
        var chunkBodies = new List<string>();
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == System.Net.Http.HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated(
                    rec.Body.Contains("\"inbound_only\":true") ? "job_in" : "job_sess"));
            if (rec.Path == "/v1/jobs/job_in") return ResponseSpec.Ok(Bodies.InboundAllowed());
            if (rec.Path.EndsWith("/chunks"))
            {
                chunkBodies.Add(rec.Body);
                using var doc = System.Text.Json.JsonDocument.Parse(rec.Body);
                int seq = doc.RootElement.GetProperty("sequence").GetInt32();
                return ResponseSpec.Ok(
                    "{\"sequence\":" + seq + ",\"accepted\":true,\"emits\":[{\"content\":\"\",\"blocked\":true,\"final\":true,"
                    + "\"block_message\":\"blocked\",\"triggered_rules\":[]}],\"finished\":false}");
            }
            return ResponseSpec.Error(System.Net.HttpStatusCode.NotFound, "not_found");
        });

        using var cts = new CancellationTokenSource();
        var events = await MutationKillTestsShared.CollectBounded(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "hi",
            RawStreamFactory = ct => MutationKillTestsShared.Endless(ct),
        }, cts.Token), cts);

        Assert.Single(events);
        Assert.IsType<Blocked>(events[0]);
        // Exactly ONE content submission — the blocked branch's own break
        // stopped the loop; the only permitted extra wire call is the
        // defensive session ABORT (empty content, is_final) that a
        // finished:false response leaves necessary.
        var contentPosts = chunkBodies.Where(b => !b.Contains("\"content\":\"\"")).ToList();
        Assert.Single(contentPosts);
        foreach (var extra in chunkBodies.Where(b => b.Contains("\"content\":\"\"")))
            Assert.Contains("\"is_final\":true", extra);
    }

    // -- block ON the final flush (previously no-coverage L381) ---------------

    [Fact]
    public async Task Block_on_the_final_flush_is_the_single_terminal()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == System.Net.Http.HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated(
                    rec.Body.Contains("\"inbound_only\":true") ? "job_in" : "job_sess"));
            if (rec.Path == "/v1/jobs/job_in") return ResponseSpec.Ok(Bodies.InboundAllowed());
            if (rec.Path.EndsWith("/chunks"))
            {
                using var doc = System.Text.Json.JsonDocument.Parse(rec.Body);
                int seq = doc.RootElement.GetProperty("sequence").GetInt32();
                bool isFinal = doc.RootElement.GetProperty("is_final").GetBoolean();
                if (isFinal) return ResponseSpec.Ok(Bodies.ChunkBlocked(seq)); // block on the FLUSH
                string content = doc.RootElement.TryGetProperty("content", out var c) ? (c.GetString() ?? "") : "";
                return ResponseSpec.Ok(Bodies.ChunkEmit(seq, content));
            }
            return ResponseSpec.Error(System.Net.HttpStatusCode.NotFound, "not_found");
        });

        var events = new List<CollieStreamEvent>();
        await foreach (var ev in client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "hi",
            RawStreamFactory = _ => TestSupportStreams.Of("a"),
        }))
            events.Add(ev);

        Assert.Equal(1, events.Count(e => e is Finished or Blocked or InputBlocked));
        Assert.IsType<Blocked>(events[^1]);
        Assert.DoesNotContain(events, e => e is Finished);
    }

    // -- BufferedFailure set membership (survivors L42) ------------------------

    [Theory]
    [InlineData("failed")]
    [InlineData("expired")]
    public async Task Buffered_poll_reaching_a_failure_status_throws(string status)
    {
        // The failure SET is verdict-table adjacent: a string mutant
        // emptied "expired" and nothing failed — no test ever polled a
        // buffered job into either failure status.
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated(
                    rec.Body.Contains("\"inbound_only\":true") ? "job_in" : "job_buf"));
            if (rec.Path == "/v1/jobs/job_in") return ResponseSpec.Ok(Bodies.InboundAllowed());
            if (rec.Path == "/v1/jobs/job_buf")
                return ResponseSpec.Ok("{\"status\":\"" + status + "\"}");
            return ResponseSpec.Error(System.Net.HttpStatusCode.NotFound, "not_found");
        });

        await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
            {
                Input = "hi",
                RawStreamFactory = _ => TestSupportStreams.Of("a"),
            }));
    }

    // -- RequireStreaming's unsupported branch (NoCov L196-197, L124) ----------

    [Fact]
    public async Task Require_streaming_on_an_unsupported_policy_throws_typed()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/streaming/preflight")
                return ResponseSpec.Ok(Bodies.PreflightUnsupported("plan_not_entitled"));
            return ResponseSpec.Error(System.Net.HttpStatusCode.NotFound, "not_found");
        });

        await Assert.ThrowsAsync<PlanNotEntitledException>(async () =>
        {
            await foreach (var _ in client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "hi",
                RawStreamFactory = _ => TestSupportStreams.Of("never"),
                RequireStreaming = true,
            })) { }
        });
    }
}

internal static class MutationKillTestsShared
{
    public static async IAsyncEnumerable<string> Endless(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        int i = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return $"d{i++}";
        }
    }

    public static async Task<List<CollieStreamEvent>> CollectBounded(
        IAsyncEnumerable<CollieStreamEvent> stream, CancellationTokenSource cts)
    {
        var task = Task.Run(async () =>
        {
            var list = new List<CollieStreamEvent>();
            await foreach (var ev in stream) list.Add(ev);
            return list;
        });
        try
        {
            return await task.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (TimeoutException)
        {
            cts.Cancel();
            try { await task.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
            throw;
        }
    }
}

internal static class TestSupportStreams
{
    public static async IAsyncEnumerable<string> Of(params string[] deltas)
    {
        foreach (var d in deltas) { await Task.Yield(); yield return d; }
    }
}
