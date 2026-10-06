using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Xunit;

namespace CollieAi.Client.Tests;

/// <summary>
/// The cross-language conformance deliverable of RFC mcp-gateway §6.5: every
/// vector in <c>sdk/conformance/mcp_canonical_vectors.json</c> must produce the
/// SAME canonical text / digest / rejection reason here as in the Python
/// reference (<c>tests/unit/test_mcp_canonical.py</c>). The vector file is the
/// oracle — it was authored by hand from the rules, never generated from either
/// implementation.
/// </summary>
public class McpCanonicalTests
{
    private static JsonDocument LoadVectors()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "mcp_canonical_vectors.json");
        return JsonDocument.Parse(File.ReadAllText(path));
    }

    private static IReadOnlyDictionary<string, string>? HeaderParams(JsonElement c)
    {
        if (!c.TryGetProperty("header_params", out var hp))
        {
            return null;
        }
        var dict = new Dictionary<string, string>();
        foreach (var p in hp.EnumerateObject())
        {
            dict[p.Name] = p.Value.GetString()!;
        }
        return dict;
    }

    public static IEnumerable<object?[]> AcceptedVectors()
    {
        using var doc = LoadVectors();
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            if (c.TryGetProperty("reject", out _))
            {
                continue;
            }
            yield return new object?[]
            {
                c.GetProperty("name").GetString()!,
                c.GetProperty("input").GetString()!,
                HeaderParams(c),
                c.GetProperty("canonical").GetString()!,
                c.GetProperty("digest").GetString()!,
                c.GetProperty("header_mirrored").EnumerateArray().Select(e => e.GetString()!).ToArray(),
            };
        }
    }

    public static IEnumerable<object?[]> RejectedVectors()
    {
        using var doc = LoadVectors();
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            if (!c.TryGetProperty("reject", out var reason))
            {
                continue;
            }
            yield return new object?[]
            {
                c.GetProperty("name").GetString()!,
                c.GetProperty("input").GetString()!,
                HeaderParams(c),
                reason.GetString()!,
            };
        }
    }

    [Fact]
    public void Vector_file_pins_the_version_and_depth_this_implementation_claims()
    {
        using var doc = LoadVectors();
        Assert.Equal(McpPayloadCanonicalizer.Version, doc.RootElement.GetProperty("canonical_version").GetString());
        Assert.Equal(McpPayloadCanonicalizer.MaxDepth, doc.RootElement.GetProperty("max_depth").GetInt32());
        // A vector suite with fewer than a few dozen cases has been truncated.
        Assert.True(doc.RootElement.GetProperty("cases").GetArrayLength() >= 40);
    }

    [Theory]
    [MemberData(nameof(AcceptedVectors))]
    public void Accepted_vectors_canonicalize_identically(
        string name, string input, IReadOnlyDictionary<string, string>? headers,
        string canonical, string digest, string[] mirrored)
    {
        var got = McpPayloadCanonicalizer.Canonicalize(input, headers);
        Assert.True(canonical == got.Text, $"{name}: canonical text differs\n expected {canonical}\n got      {got.Text}");
        Assert.True(digest == got.Digest, $"{name}: digest differs");
        Assert.Equal(mirrored, got.HeaderMirrored);
        // The vector's digest is independently recomputable from its canonical text.
        string recomputed = McpPayloadCanonicalizer.Version + ":" +
            Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical))).ToLowerInvariant();
        Assert.Equal(digest, recomputed);
        // Idempotence: canonical text is its own canonical form.
        Assert.Equal(got.Text, McpPayloadCanonicalizer.Canonicalize(got.Text).Text);
        // The byte overload agrees with the string overload.
        Assert.Equal(got.Digest, McpPayloadCanonicalizer.Canonicalize(Encoding.UTF8.GetBytes(input), headers).Digest);
    }

    [Theory]
    [MemberData(nameof(RejectedVectors))]
    public void Rejected_vectors_refuse_with_the_shared_reason(
        string name, string input, IReadOnlyDictionary<string, string>? headers, string reason)
    {
        var ex = Assert.Throws<McpCanonicalizationException>(() => McpPayloadCanonicalizer.Canonicalize(input, headers));
        Assert.True(reason == ex.Reason, $"{name}: expected {reason}, got {ex.Reason} ({ex.Message})");
        Assert.Contains(reason, McpPayloadCanonicalizer.RejectionReasons);
    }

    [Fact]
    public void Invalid_utf8_bytes_are_not_json()
    {
        byte[] bad = { (byte)'"', 0xC3, 0x28, (byte)'"' };
        var ex = Assert.Throws<McpCanonicalizationException>(() => McpPayloadCanonicalizer.Canonicalize(bad));
        Assert.Equal("not_json", ex.Reason);
    }

    [Fact]
    public void Unpaired_surrogate_in_a_dotnet_string_is_not_json()
    {
        string lone = "\"" + '\uD800' + "\"";
        var ex = Assert.Throws<McpCanonicalizationException>(() => McpPayloadCanonicalizer.Canonicalize(lone));
        Assert.Equal("not_json", ex.Reason);
    }

    [Theory]
    [InlineData("0", "0")]
    [InlineData("-0.0e7", "0")]
    [InlineData("1.0", "1")]
    [InlineData("12.50e1", "125")]
    [InlineData("1e21", "1e+21")]
    [InlineData("1e20", "100000000000000000000")]
    [InlineData("1e-7", "1e-7")]
    [InlineData("1e-6", "0.000001")]
    [InlineData("9007199254740993", "9007199254740993")]
    [InlineData("123456789e-20", "1.23456789e-12")]
    public void Number_layout_matches_the_rule_table(string literal, string expected)
    {
        Assert.Equal(expected, McpPayloadCanonicalizer.CanonicalNumber(literal));
    }
}
