using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

public class ProtectStreamTests
{
    // preflight + input-check job + session job + echoing chunk endpoint.
    private static Func<RecordedRequest, int, ResponseSpec> Route(
        string preflightBody, string inboundBody, Func<RecordedRequest, ResponseSpec>? chunkResponder = null) => (rec, _) =>
    {
        if (rec.Path == "/v1/streaming/preflight")
            return ResponseSpec.Ok(preflightBody);
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return rec.Body.Contains("\"inbound_only\":true")
                ? ResponseSpec.Ok(Bodies.JobCreated("job_in"))
                : ResponseSpec.Ok(Bodies.JobCreated("job_sess"));
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get)
            return ResponseSpec.Ok(inboundBody);
        if (rec.Path.EndsWith("/chunks"))
            return (chunkResponder ?? Responders.EchoChunk)(rec);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    private static async Task<List<CollieStreamEvent>> CollectAsync(IAsyncEnumerable<CollieStreamEvent> stream)
    {
        var list = new List<CollieStreamEvent>();
        await foreach (var ev in stream)
            list.Add(ev);
        return list;
    }

    private static string SafeText(IEnumerable<CollieStreamEvent> events) =>
        string.Concat(events.OfType<SafeDelta>().Select(d => d.Text));

    [Fact]
    public async Task Happy_path_streams_safe_deltas_and_finishes_calling_factory_once()
    {
        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("Hello", " world");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        Assert.Equal(1, factory.Calls);
        Assert.Equal("Hello world", SafeText(events));
        Assert.IsType<Finished>(events[^1]);
        Assert.DoesNotContain(events, e => e is Blocked or InputBlocked);
    }

    [Fact]
    public async Task Input_block_prevents_provider_factory_invocation()
    {
        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundBlocked()));
        var factory = new CountingFactory("should", "not", "run");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        Assert.Equal(0, factory.Calls);
        var blocked = Assert.IsType<InputBlocked>(Assert.Single(events));
        Assert.Equal("blocked by policy", blocked.BlockMessage);
    }

    private static List<JsonDocument> SessionCreateBodies(FakeHandler handler) =>
        handler.Requests
            .Where(r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post
                        && !r.Body.Contains("\"inbound_only\":true"))
            .Select(r => JsonDocument.Parse(r.Body))
            .ToList();

    [Fact]
    public async Task Session_create_carries_the_gates_job_id_as_input_job_id()
    {
        // The duplicate-inbound contract: the session job proves its
        // prompt was gated instead of paying a second inbound pass;
        // message_input stays (the chunk path's guard prompt context reads it).
        var (client, handler) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("ok");

        await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        var body = Assert.Single(SessionCreateBodies(handler));
        Assert.Equal("job_in", body.RootElement.GetProperty("input_job_id").GetString());
        Assert.Equal("prompt", body.RootElement.GetProperty("message_input").GetString());
    }

    [Fact]
    public async Task No_gate_reference_without_an_input_check()
    {
        // CheckInput=false ran no gate, so there is no proof to send — the
        // field must be ABSENT (an older server ignores it; the current one
        // would loudly reject a bogus claim).
        var (client, handler) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("ok");

        await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            CheckInput = false,
            RawStreamFactory = factory.Factory,
        }));

        var body = Assert.Single(SessionCreateBodies(handler));
        Assert.False(body.RootElement.TryGetProperty("input_job_id", out _));
    }

    [Fact]
    public async Task Precomputed_input_result_supplies_its_own_job_id()
    {
        var (client, handler) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("ok");

        await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
            InputResult = new InputModerationResult
            {
                Allowed = true, Blocked = false, OriginalText = "prompt", JobId = "job_pre",
            },
        }));

        var body = Assert.Single(SessionCreateBodies(handler));
        Assert.Equal("job_pre", body.RootElement.GetProperty("input_job_id").GetString());
    }

    [Fact]
    public async Task Precomputed_input_result_without_job_id_omits_the_reference()
    {
        var (client, handler) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("ok");

        await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
            InputResult = new InputModerationResult
            {
                Allowed = true, Blocked = false, OriginalText = "prompt",
            },
        }));

        var body = Assert.Single(SessionCreateBodies(handler));
        Assert.False(body.RootElement.TryGetProperty("input_job_id", out _));
    }

    // -----------------------------------------------------------------------
    // The 409 input_gate_stale protocol (server claim contract §10.2)
    // -----------------------------------------------------------------------
    private sealed class StaleCounters
    {
        public int ModCalls;
        public int StaleServed;
    }

    private static Func<RecordedRequest, int, ResponseSpec> StaleRoute(
        StaleCounters counters, int staleTimes, bool blockOnRegate = false,
        string code = "input_gate_stale") => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
        {
            if (rec.Body.Contains("\"inbound_only\":true"))
            {
                counters.ModCalls++;
                return ResponseSpec.Ok(Bodies.JobCreated($"job_in{counters.ModCalls}"));
            }
            if (rec.Body.Contains("\"input_job_id\"") && counters.StaleServed < staleTimes)
            {
                counters.StaleServed++;
                return new ResponseSpec(HttpStatusCode.Conflict,
                    "{\"error\":{\"message\":\"claim refused\",\"type\":\"" + code + "\",\"code\":\"" + code + "\"}}");
            }
            return ResponseSpec.Ok(Bodies.JobCreated("job_sess"));
        }
        if (rec.Path.StartsWith("/v1/jobs/job_in") && rec.Method == HttpMethod.Get)
        {
            bool blocked = blockOnRegate && rec.Path.EndsWith("job_in2");
            return ResponseSpec.Ok(blocked
                ? Bodies.InboundBlocked("blocked by the CURRENT policy")
                : Bodies.InboundAllowed());
        }
        if (rec.Path.EndsWith("/chunks"))
            return Responders.EchoChunk(rec);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Fact]
    public async Task Stale_gate_regates_once_before_the_provider()
    {
        var counters = new StaleCounters();
        var (client, handler) = TestClient.Create(StaleRoute(counters, staleTimes: 1));
        var factory = new CountingFactory("ok");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        Assert.Equal(2, counters.ModCalls); // original gate + exactly one re-gate
        var sessionBodies = SessionCreateBodies(handler);
        Assert.Equal(2, sessionBodies.Count);
        Assert.Equal("job_in1", sessionBodies[0].RootElement.GetProperty("input_job_id").GetString());
        Assert.Equal("job_in2", sessionBodies[1].RootElement.GetProperty("input_job_id").GetString());
        Assert.Equal("ok", SafeText(events));
        Assert.Equal(1, factory.Calls);
    }

    [Fact]
    public async Task A_second_stale_throws_with_zero_provider_calls()
    {
        // Round 5: a stale on the RETRIED create is POSITIVE PROOF of
        // policy churn — the round-4 claimless downgrade would have raced
        // the provider against a proven drift. Typed error, provider never
        // started.
        var counters = new StaleCounters();
        var (client, _) = TestClient.Create(StaleRoute(counters, staleTimes: 2));
        var factory = new CountingFactory("never");

        var exc = await Assert.ThrowsAsync<CollieApiException>(() =>
            CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
            })));

        Assert.Equal("input_gate_stale", exc.Code);
        Assert.Equal(2, counters.ModCalls); // exactly one re-gate, no loop
        Assert.Equal(0, factory.Calls);     // provider NEVER started
    }

    [Fact]
    public async Task Stale_then_unverifiable_downgrades_to_claimless()
    {
        // The one second-409 that still downgrades: a pin outage beginning
        // mid-turn — nothing says the fresh re-gate verdict is wrong.
        var codes = new[] { "input_gate_stale", "input_gate_unverifiable" };
        int served = 0, modCalls = 0;
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            {
                if (rec.Body.Contains("\"inbound_only\":true"))
                {
                    modCalls++;
                    return ResponseSpec.Ok(Bodies.JobCreated($"job_in{modCalls}"));
                }
                if (rec.Body.Contains("\"input_job_id\"") && served < codes.Length)
                {
                    string code = codes[served++];
                    return new ResponseSpec(HttpStatusCode.Conflict,
                        "{\"error\":{\"message\":\"refused\",\"type\":\"" + code + "\",\"code\":\"" + code + "\"}}");
                }
                return ResponseSpec.Ok(Bodies.JobCreated("job_sess"));
            }
            if (rec.Path.StartsWith("/v1/jobs/job_in") && rec.Method == HttpMethod.Get)
                return ResponseSpec.Ok(Bodies.InboundAllowed());
            if (rec.Path.EndsWith("/chunks"))
                return Responders.EchoChunk(rec);
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        });
        var factory = new CountingFactory("ok");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        var sessionBodies = SessionCreateBodies(handler);
        Assert.Equal(3, sessionBodies.Count);
        Assert.False(sessionBodies[2].RootElement.TryGetProperty("input_job_id", out _)); // claimless
        Assert.Equal("ok", SafeText(events));
    }

    private static Func<RecordedRequest, int, ResponseSpec> MaskingRoute(string filtered) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return ResponseSpec.Ok(Bodies.JobCreated(
                rec.Body.Contains("\"inbound_only\":true") ? "job_in" : "job_sess"));
        if (rec.Path.StartsWith("/v1/jobs/job_in") && rec.Method == HttpMethod.Get)
            return ResponseSpec.Ok(
                "{\"status\":\"completed\",\"inbound_result\":{\"allowed\":true,\"blocked\":false,\"filtered_content\":"
                + JsonSerializer.Serialize(filtered) + ",\"triggered_rules\":[]}}");
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Theory]
    [InlineData("my [MASKED] prompt")]
    [InlineData("")] // the full wipe — `!=`, never null/empty checks
    public async Task Masked_input_fails_closed_before_the_provider(string filtered)
    {
        var (client, handler) = TestClient.Create(MaskingRoute(filtered));
        var factory = new CountingFactory("never");

        await Assert.ThrowsAsync<MaskedInputException>(() =>
            CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "my secret prompt",
                RawStreamFactory = factory.Factory,
            })));

        Assert.Equal(0, factory.Calls);
        Assert.Empty(SessionCreateBodies(handler)); // refused before any session create
    }

    [Fact]
    public async Task Masked_input_fails_closed_in_buffered_too()
    {
        var (client, _) = TestClient.Create(MaskingRoute("my [MASKED] prompt"));
        var factory = new CountingFactory("never");

        await Assert.ThrowsAsync<MaskedInputException>(() =>
            client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
            {
                Input = "my secret prompt",
                RawStreamFactory = factory.Factory,
            }));
        Assert.Equal(0, factory.Calls);
    }

    [Theory]
    [InlineData("my [MASKED] prompt")]
    [InlineData("")]
    public async Task Regate_mask_fails_closed_before_the_provider(string filtered)
    {
        // Round 6: the mask check must hold on the LADDER's re-gate too.
        int modCalls = 0, staleServed = 0;
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            {
                if (rec.Body.Contains("\"inbound_only\":true"))
                {
                    modCalls++;
                    return ResponseSpec.Ok(Bodies.JobCreated($"job_in{modCalls}"));
                }
                if (rec.Body.Contains("\"input_job_id\"") && staleServed < 1)
                {
                    staleServed++;
                    return new ResponseSpec(HttpStatusCode.Conflict,
                        "{\"error\":{\"message\":\"stale\",\"type\":\"input_gate_stale\",\"code\":\"input_gate_stale\"}}");
                }
                return ResponseSpec.Ok(Bodies.JobCreated("job_sess"));
            }
            if (rec.Path.StartsWith("/v1/jobs/job_in") && rec.Method == HttpMethod.Get)
            {
                bool masked = rec.Path.EndsWith("job_in2");
                string inbound = masked
                    ? "{\"allowed\":true,\"blocked\":false,\"filtered_content\":" + JsonSerializer.Serialize(filtered) + ",\"triggered_rules\":[]}"
                    : "{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]}";
                return ResponseSpec.Ok("{\"status\":\"completed\",\"inbound_result\":" + inbound + "}");
            }
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        });
        var factory = new CountingFactory("never");

        await Assert.ThrowsAsync<MaskedInputException>(() =>
            CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "my secret prompt",
                RawStreamFactory = factory.Factory,
            })));

        Assert.Equal(2, modCalls);
        Assert.Equal(0, factory.Calls);
    }

    [Theory]
    [InlineData("my [MASKED] prompt")]
    [InlineData("")]
    public async Task The_masking_recipe_streams_the_filtered_prompt(string filtered)
    {
        // The POSITIVE half of the recipe: gate manually, build the factory
        // over FilteredText (null-safe — "" is a legitimate full wipe),
        // pass InputResult. The captured prompt PROVES the model gets the
        // filtered text.
        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        const string original = "my secret prompt";
        string promptForModel = filtered;
        var usedPrompts = new List<string>();

        async IAsyncEnumerable<string> Factory([EnumeratorCancellation] CancellationToken ct = default)
        {
            usedPrompts.Add(promptForModel);
            yield return "ok";
            await Task.CompletedTask;
        }

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = original,
            RawStreamFactory = Factory,
            InputResult = new InputModerationResult
            {
                Allowed = true, Blocked = false,
                OriginalText = original, FilteredText = filtered, JobId = "job_pre",
            },
        }));

        Assert.Equal("ok", SafeText(events));
        Assert.Equal(new[] { filtered }, usedPrompts); // the model saw the FILTERED prompt
        Assert.DoesNotContain(original, usedPrompts);
    }

    [Fact]
    public async Task The_InputResult_path_is_exempt_from_the_mask_fail_closed()
    {
        // The caller demonstrably holds FilteredText and owns the factory.
        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("ok");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "my secret prompt",
            RawStreamFactory = factory.Factory,
            InputResult = new InputModerationResult
            {
                Allowed = true, Blocked = false,
                OriginalText = "my secret prompt",
                FilteredText = "my [MASKED] prompt",
                JobId = "job_pre",
            },
        }));

        Assert.Equal("ok", SafeText(events));
    }

    [Fact]
    public async Task Unverifiable_downgrades_immediately_without_a_regate()
    {
        // Round 4: a pin outage makes re-gating pointless (a fresh gate is
        // just as unpinned) — go straight to the claimless session.
        var counters = new StaleCounters();
        var (client, handler) = TestClient.Create(StaleRoute(
            counters, staleTimes: 1, code: "input_gate_unverifiable"));
        var factory = new CountingFactory("ok");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        Assert.Equal(1, counters.ModCalls); // the original gate only — NO re-gate
        var sessionBodies = SessionCreateBodies(handler);
        Assert.Equal(2, sessionBodies.Count);
        Assert.Equal("job_in1", sessionBodies[0].RootElement.GetProperty("input_job_id").GetString());
        Assert.False(sessionBodies[1].RootElement.TryGetProperty("input_job_id", out _)); // claimless
        Assert.Equal("ok", SafeText(events));
        Assert.Equal(1, factory.Calls);
    }

    [Fact]
    public async Task A_regate_block_yields_InputBlocked_without_provider_spend()
    {
        var counters = new StaleCounters();
        var (client, _) = TestClient.Create(StaleRoute(counters, staleTimes: 1, blockOnRegate: true));
        var factory = new CountingFactory("never");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        var blocked = Assert.IsType<InputBlocked>(Assert.Single(events));
        Assert.Equal("blocked by the CURRENT policy", blocked.BlockMessage);
        Assert.Equal(0, factory.Calls);
    }

    [Theory]
    [InlineData("input_gate_stale")]
    [InlineData("input_gate_unverifiable")]
    public async Task Claim_refusal_with_external_InputResult_throws_no_auto_remedy(string code)
    {
        // Rounds 3–4: an external InputResult may carry a verdict produced
        // WITH a context this wrapper never saw; neither a prompt-only
        // re-gate nor a silent claimless downgrade is honest. The caller
        // re-gates with its own context.
        var counters = new StaleCounters();
        var (client, _) = TestClient.Create(StaleRoute(counters, staleTimes: 1, code: code));
        var factory = new CountingFactory("never");

        var exc = await Assert.ThrowsAsync<CollieApiException>(() =>
            CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
                InputResult = new InputModerationResult
                {
                    Allowed = true, Blocked = false, OriginalText = "prompt", JobId = "job_pre",
                },
            })));

        Assert.Equal(code, exc.Code);
        Assert.Equal(0, counters.ModCalls); // NO automatic re-gate
        Assert.Equal(0, factory.Calls);     // provider never started
    }

    [Fact]
    public async Task Input_gate_claimed_propagates_untouched()
    {
        // The counters pin "untouched": adding the code to the refusal
        // ladder would run a billed re-gate + a second create before the
        // same exception, and only the counts catch that (the code-only
        // assertion survived exactly that mutation — post-round-10
        // workflow review, mirrored from the Python/Node twins).
        int gateCreates = 0, sessionCreates = 0;
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            {
                if (rec.Body.Contains("\"inbound_only\":true"))
                {
                    gateCreates++;
                    return ResponseSpec.Ok(Bodies.JobCreated("job_in"));
                }
                sessionCreates++;
                return new ResponseSpec(HttpStatusCode.Conflict,
                    "{\"error\":{\"message\":\"already claimed\",\"type\":\"input_gate_claimed\",\"code\":\"input_gate_claimed\"}}");
            }
            if (rec.Path.StartsWith("/v1/jobs/job_in") && rec.Method == HttpMethod.Get)
                return ResponseSpec.Ok(Bodies.InboundAllowed());
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        });
        var factory = new CountingFactory("never");

        var exc = await Assert.ThrowsAsync<CollieApiException>(() =>
            CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
            })));

        Assert.Equal("input_gate_claimed", exc.Code);
        Assert.Equal(1, gateCreates);    // the wrapper's own gate — no re-gate
        Assert.Equal(1, sessionCreates); // no second create attempt
        Assert.Equal(0, factory.Calls);  // provider never started
    }

    [Fact]
    public async Task Only_collie_released_text_is_yielded_never_raw_provider_text()
    {
        static ResponseSpec Mask(RecordedRequest rec)
        {
            using var doc = JsonDocument.Parse(rec.Body);
            int seq = doc.RootElement.GetProperty("sequence").GetInt32();
            bool isFinal = doc.RootElement.GetProperty("is_final").GetBoolean();
            return isFinal ? ResponseSpec.Ok(Bodies.ChunkFinal(seq)) : ResponseSpec.Ok(Bodies.ChunkEmit(seq, "MASKED"));
        }

        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed(), Mask));
        var factory = new CountingFactory("my secret SSN 123-45-6789");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        string safe = SafeText(events);
        Assert.Equal("MASKED", safe);
        Assert.DoesNotContain("secret", safe);
        Assert.DoesNotContain("123-45-6789", safe);
    }

    [Fact]
    public async Task Output_block_is_a_terminal_event()
    {
        var (client, _) = TestClient.Create(Route(
            Bodies.PreflightStreaming(), Bodies.InboundAllowed(),
            rec =>
            {
                using var doc = JsonDocument.Parse(rec.Body);
                int seq = doc.RootElement.GetProperty("sequence").GetInt32();
                return ResponseSpec.Ok(Bodies.ChunkBlocked(seq));
            }));
        var factory = new CountingFactory("anything");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }));

        var blockedEvent = Assert.IsType<Blocked>(Assert.Single(events));
        // F1 parity with Python/Node: the rule's configured message reaches the event.
        Assert.Equal("Custom rule block message", blockedEvent.BlockMessage);
        Assert.DoesNotContain(events, e => e is Finished);
    }

    [Fact]
    public async Task Require_streaming_throws_when_policy_buffers()
    {
        var (client, _) = TestClient.Create(Route(Bodies.PreflightBuffered(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("x");

        await Assert.ThrowsAsync<BufferedFallbackRequiredException>(() => CollectAsync(
            client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
                RequireStreaming = true,
            })));

        Assert.Equal(0, factory.Calls);
    }

    [Fact]
    public async Task Default_path_does_not_preflight_and_surfaces_unstreamable_policy_on_push()
    {
        // The default path streams optimistically (no preflight, matching the
        // Python/Node SDKs); a non-streamable policy is reported by the chunk
        // endpoint as a fatal ChunkStreamingUnsupportedException.
        var (client, handler) = TestClient.Create(Route(
            Bodies.PreflightStreaming(), Bodies.InboundAllowed(),
            _ => ResponseSpec.Error(HttpStatusCode.BadRequest, "chunk_streaming_unsupported")));
        var factory = new CountingFactory("hello");

        await Assert.ThrowsAsync<ChunkStreamingUnsupportedException>(() => CollectAsync(
            client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
            })));

        Assert.Equal(0, handler.CountWhere(r => r.Path == "/v1/streaming/preflight")); // default path never preflights
    }

    [Fact]
    public async Task Factory_returning_null_is_rejected()
    {
        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));

        // The factory is non-null (passes sync validation) but returns null at call time.
        var stream = client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = _ => null!,
        });

        await Assert.ThrowsAsync<ProviderStreamFactoryRequiredException>(() => CollectAsync(stream));
    }

    [Fact]
    public async Task Cleanup_after_block_cancels_provider_and_does_not_hang()
    {
        // Provider yields one delta, then blocks until its token is cancelled. If
        // cleanup awaited the in-flight read before cancelling, this would deadlock.
        static async IAsyncEnumerable<string> Provider([EnumeratorCancellation] CancellationToken token)
        {
            yield return "hello";
            await Task.Delay(Timeout.Infinite, token); // unblocks only on cancellation
            yield return "never";
        }

        var (client, _) = TestClient.Create(Route(
            Bodies.PreflightStreaming(), Bodies.InboundAllowed(),
            rec =>
            {
                using var doc = JsonDocument.Parse(rec.Body);
                int seq = doc.RootElement.GetProperty("sequence").GetInt32();
                return ResponseSpec.Ok(Bodies.ChunkBlocked(seq)); // first push blocks → stream terminates early
            }));

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = Provider,
        })).WaitAsync(TimeSpan.FromSeconds(5)); // converts a cleanup deadlock into a test failure, not a hang

        Assert.IsType<Blocked>(Assert.Single(events));
    }

    [Fact]
    public void Missing_factory_is_rejected_synchronously()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));

        // ProtectStreamAsync validates before returning the iterator.
        Assert.Throws<ProviderStreamFactoryRequiredException>(() =>
            client.Streaming.ProtectStreamAsync(new ProtectStreamRequest { Input = "x", RawStreamFactory = null! }));
    }

    [Fact]
    public void Mismatched_input_result_is_rejected_synchronously()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));
        var factory = new CountingFactory("x");
        var wrong = new InputModerationResult { Allowed = true, Blocked = false, OriginalText = "DIFFERENT" };

        Assert.Throws<ArgumentException>(() =>
            client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
                InputResult = wrong,
            }));
    }

    [Fact]
    public async Task InputResult_with_CheckInput_false_is_rejected_in_both_wrappers()
    {
        // Round 10: this combination used to be silently ignored (no claim
        // sent, the session re-filtered the prompt) — exactly the shape a
        // CheckInput=false integration would produce by accident. Now loud,
        // before any side effect (no factory call, no job created).
        var (client, handler) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));
        var factory = new CountingFactory("x");
        var ir = new InputModerationResult { Allowed = true, Blocked = false, OriginalText = "prompt" };

        var ex1 = Assert.Throws<ArgumentException>(() =>
            client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
                InputResult = ir,
                CheckInput = false,
            }));
        Assert.Contains("CheckInput = true", ex1.Message);

        var ex2 = await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
                InputResult = ir,
                CheckInput = false,
            }));
        Assert.Contains("CheckInput = true", ex2.Message);

        Assert.Equal(0, factory.Calls);
        Assert.DoesNotContain(handler.Requests, r => r.Path == "/v1/jobs");
    }

    [Fact]
    public async Task ProtectStream_forwards_context_to_input_gate()
    {
        var (client, handler) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundAllowed()));
        var factory = new CountingFactory("ok");

        await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
            Context = new Dictionary<string, object>
            {
                ["transaction"] = new Dictionary<string, object> { ["titleNew"] = "hi" },
            },
            ContextFormat = "json",
        }));

        var inbound = handler.Requests.First(
            r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post && r.Body.Contains("\"inbound_only\":true"));
        Assert.Contains("\"context\":{\"transaction\":{\"titleNew\":\"hi\"}}", inbound.Body);
        Assert.Contains("\"context_format\":\"json\"", inbound.Body);
    }

    [Fact]
    public void ProtectStream_rejects_context_with_input_result()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));
        var factory = new CountingFactory("x");
        var allowed = new InputModerationResult { Allowed = true, Blocked = false, OriginalText = "prompt" };

        Assert.Throws<ArgumentException>(() =>
            client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
                InputResult = allowed,
                Context = new Dictionary<string, object> { ["a"] = "b" },
            }));
    }

    [Fact]
    public async Task ProtectBuffered_rejects_context_with_input_result()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));
        var factory = new CountingFactory("x");
        var allowed = new InputModerationResult { Allowed = true, Blocked = false, OriginalText = "prompt" };

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
            {
                Input = "prompt",
                RawStreamFactory = factory.Factory,
                InputResult = allowed,
                Context = new Dictionary<string, object> { ["a"] = "b" },
            }));
    }

    [Fact]
    public async Task ProtectStream_input_block_carries_context()
    {
        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundContextBlocked()));
        var factory = new CountingFactory("x");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "hi",
            RawStreamFactory = factory.Factory,
            Context = new Dictionary<string, object>
            {
                ["transaction"] = new Dictionary<string, object> { ["title"] = "y" },
            },
        }));

        var blocked = Assert.IsType<InputBlocked>(Assert.Single(events));
        Assert.Equal("context", blocked.BlockedBy);
        Assert.NotNull(blocked.Context);
        Assert.Equal("blocked", blocked.Context!.Status);
        Assert.Equal("/transaction/title", blocked.Context.TriggeringPointer);
        Assert.Equal(0, factory.Calls);  // input blocked -> provider never ran
    }

    [Fact]
    public async Task ProtectBuffered_input_block_carries_context()
    {
        var (client, _) = TestClient.Create(Route(Bodies.PreflightStreaming(), Bodies.InboundContextBlocked()));
        var factory = new CountingFactory("x");

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "hi",
            RawStreamFactory = factory.Factory,
            Context = new Dictionary<string, object> { ["a"] = "b" },
        });

        Assert.True(result.InputBlocked);
        Assert.Equal("context", result.BlockedBy);
        Assert.NotNull(result.Context);
        Assert.Equal("blocked", result.Context!.Status);
        Assert.Equal("/transaction/title", result.Context.TriggeringPointer);
    }

    [Fact]
    public async Task ProtectStream_finished_carries_input_context()
    {
        // monitored/degraded-but-allowed context is observable on the success
        // terminal (Finished), not only on a block (Slice 1f review).
        var (client, _) = TestClient.Create(Route(
            Bodies.PreflightStreaming(),
            Bodies.InboundWithContext("monitored", blocked: false, blockedBy: "none")));
        var factory = new CountingFactory("hello");

        var events = await CollectAsync(client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "hi",
            RawStreamFactory = factory.Factory,
            Context = new Dictionary<string, object> { ["a"] = "b" },
        }));

        var finished = Assert.IsType<Finished>(events[^1]);
        Assert.Equal("none", finished.BlockedBy);
        Assert.NotNull(finished.Context);
        Assert.Equal("monitored", finished.Context!.Status);
    }
}
