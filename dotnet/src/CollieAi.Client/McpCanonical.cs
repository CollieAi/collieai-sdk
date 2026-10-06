using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace CollieAi;

/// <summary>
/// A payload the MCP canonical form refuses. <see cref="Reason"/> is one of the
/// closed labels in <see cref="McpPayloadCanonicalizer.RejectionReasons"/> — the
/// same label the CollieAi server answers for the same input.
/// </summary>
public sealed class McpCanonicalizationException : Exception
{
    /// <summary>The closed rejection label (see <see cref="McpPayloadCanonicalizer.RejectionReasons"/>).</summary>
    public string Reason { get; }

    /// <summary>Creates a refusal with a closed <paramref name="reason"/> and diagnostic <paramref name="detail"/>.</summary>
    public McpCanonicalizationException(string reason, string detail)
        : base($"{reason}: {detail}")
    {
        Reason = reason;
    }
}

/// <summary>The authoritative representation of one MCP payload.</summary>
/// <param name="Text">Canonical JSON text.</param>
/// <param name="Bytes">UTF-8 bytes of <paramref name="Text"/> — the digest input.</param>
/// <param name="Digest">`mcpc1:&lt;sha256 hex&gt;` — byte-comparable with the
/// server's <c>evaluated_payload_digest</c>.</param>
/// <param name="HeaderMirrored">Names of the Era-B <c>Mcp-Param-*</c> parameters that were
/// reconciled against the body (sorted by code point).</param>
public sealed record McpCanonicalPayload(
    string Text,
    byte[] Bytes,
    string Digest,
    IReadOnlyList<string> HeaderMirrored);

/// <summary>
/// .NET reference implementation of the MCP payload canonical form, version
/// <c>mcpc1</c>. It must agree vector-by-vector with the CollieAi server's
/// implementation on the shared frozen vectors
/// (<c>mcp_canonical_vectors.json</c> in the conformance directory: expected
/// canonical text, digest and rejection reason per case).
///
/// The adapter (the datapath owner) uses this to RECOMPUTE the digest of the
/// payload it is about to forward and abort on mismatch with the digest the
/// evaluation returned — closing the evaluate-A / forward-B parser-differential
/// channel. Rules, in short: strict RFC 8259 parse (no BOM, no NaN/Infinity, no
/// lone surrogates, duplicate keys rejected, nesting ≤ 64); numbers
/// canonicalized LOSSLESSLY as exact decimals in ES6 <c>Number::toString</c>
/// layout (never through a double — <c>9007199254740993</c> survives); object
/// keys sorted by Unicode code point (UTF-8 byte order); minimal string
/// escaping; no Unicode normalization; no whitespace.
/// </summary>
public static class McpPayloadCanonicalizer
{
    /// <summary>Version tag carried in every digest.</summary>
    public const string Version = "mcpc1";

    /// <summary>C9: maximum container nesting (root container = depth 1).</summary>
    public const int MaxDepth = 64;

    /// <summary>The closed rejection vocabulary (shared with the server).</summary>
    public static readonly IReadOnlyList<string> RejectionReasons = new[]
    {
        "not_json", "duplicate_key", "depth_exceeded",
        "body_not_object", "header_param_missing", "header_param_mismatch",
    };

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static readonly Regex NumberRe = new(@"\A-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\z", RegexOptions.CultureInvariant);

    /// <summary>Canonicalize JSON text. Throws <see cref="McpCanonicalizationException"/> on refusal.</summary>
    /// <param name="json">The payload as text (a lone surrogate in it is refused as <c>not_json</c>).</param>
    /// <param name="headerParams">Era B only: the <c>Mcp-Param-*</c> values keyed by parameter name.</param>
    public static McpCanonicalPayload Canonicalize(string json, IReadOnlyDictionary<string, string>? headerParams = null)
    {
        ArgumentNullException.ThrowIfNull(json);
        byte[] bytes;
        try
        {
            bytes = StrictUtf8.GetBytes(json);
        }
        catch (EncoderFallbackException)
        {
            throw new McpCanonicalizationException("not_json", "text contains an unpaired surrogate");
        }
        return Canonicalize(bytes, headerParams);
    }

    /// <summary>Canonicalize UTF-8 JSON bytes (the form an adapter holds before forwarding).</summary>
    public static McpCanonicalPayload Canonicalize(ReadOnlySpan<byte> utf8Json, IReadOnlyDictionary<string, string>? headerParams = null)
    {
        object? tree = ParseStrict(utf8Json);
        IReadOnlyList<string> mirrored = ReconcileHeaderParams(tree, headerParams);
        string text = CanonicalText(tree);
        byte[] bytes = StrictUtf8.GetBytes(text);
        return new McpCanonicalPayload(text, bytes, Digest(bytes), mirrored);
    }

    /// <summary>C12: <c>mcpc1:</c> + lowercase hex SHA-256 over the canonical bytes.</summary>
    public static string Digest(ReadOnlySpan<byte> canonicalBytes)
        => Version + ":" + Convert.ToHexString(SHA256.HashData(canonicalBytes)).ToLowerInvariant();

    // ── parsing (C1, C2, C7, C9) ────────────────────────────────────────

    private sealed class NumberLiteral
    {
        public NumberLiteral(string literal) => Literal = literal;
        public string Literal { get; }
    }

    private sealed class CodePointComparer : IComparer<string>
    {
        public static readonly CodePointComparer Instance = new();

        public int Compare(string? x, string? y)
        {
            // C6: code-point order == UTF-8 byte order. Keys reached here through
            // GetString(), so they are well-formed and encodable.
            ReadOnlySpan<byte> a = Encoding.UTF8.GetBytes(x ?? string.Empty);
            ReadOnlySpan<byte> b = Encoding.UTF8.GetBytes(y ?? string.Empty);
            return a.SequenceCompareTo(b);
        }
    }

    private static object? ParseStrict(ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.Length >= 3 && utf8Json[0] == 0xEF && utf8Json[1] == 0xBB && utf8Json[2] == 0xBF)
        {
            throw new McpCanonicalizationException("not_json", "leading byte-order mark");
        }
        // The reader's own depth limit sits ABOVE ours so that C9 is reported as
        // depth_exceeded by our walk, never as an opaque reader error.
        var options = new JsonReaderOptions
        {
            AllowTrailingCommas = false,
            CommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = 4096,
        };
        var reader = new Utf8JsonReader(utf8Json, options);
        try
        {
            if (!reader.Read())
            {
                throw new McpCanonicalizationException("not_json", "empty input");
            }
            object? value = ReadValue(ref reader, 0);
            if (reader.Read())
            {
                throw new McpCanonicalizationException("not_json", "trailing content after the root value");
            }
            return value;
        }
        catch (JsonException ex)
        {
            throw new McpCanonicalizationException("not_json", ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            // Utf8JsonReader reports invalid UTF-8 in a string token and an
            // unpaired surrogate escape (C2) as InvalidOperationException from
            // GetString(), not as JsonException.
            throw new McpCanonicalizationException("not_json", ex.Message);
        }
    }

    private static object? ReadValue(ref Utf8JsonReader reader, int depth)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject:
            {
                if (depth + 1 > MaxDepth)
                {
                    throw new McpCanonicalizationException("depth_exceeded", $"nesting exceeds {MaxDepth}");
                }
                var obj = new SortedDictionary<string, object?>(CodePointComparer.Instance);
                while (true)
                {
                    if (!reader.Read())
                    {
                        throw new McpCanonicalizationException("not_json", "unterminated object");
                    }
                    if (reader.TokenType == JsonTokenType.EndObject)
                    {
                        return obj;
                    }
                    string key = reader.GetString()!;
                    if (obj.ContainsKey(key))
                    {
                        throw new McpCanonicalizationException("duplicate_key", $"duplicate object key '{key}'");
                    }
                    if (!reader.Read())
                    {
                        throw new McpCanonicalizationException("not_json", "unterminated object");
                    }
                    obj[key] = ReadValue(ref reader, depth + 1);
                }
            }
            case JsonTokenType.StartArray:
            {
                if (depth + 1 > MaxDepth)
                {
                    throw new McpCanonicalizationException("depth_exceeded", $"nesting exceeds {MaxDepth}");
                }
                var list = new List<object?>();
                while (true)
                {
                    if (!reader.Read())
                    {
                        throw new McpCanonicalizationException("not_json", "unterminated array");
                    }
                    if (reader.TokenType == JsonTokenType.EndArray)
                    {
                        return list;
                    }
                    list.Add(ReadValue(ref reader, depth + 1));
                }
            }
            case JsonTokenType.String:
                return reader.GetString();
            case JsonTokenType.Number:
            {
                byte[] raw = reader.HasValueSequence ? reader.ValueSequence.ToArray() : reader.ValueSpan.ToArray();
                return new NumberLiteral(Encoding.ASCII.GetString(raw));
            }
            case JsonTokenType.True:
                return true;
            case JsonTokenType.False:
                return false;
            case JsonTokenType.Null:
                return null;
            default:
                throw new McpCanonicalizationException("not_json", $"unexpected token {reader.TokenType}");
        }
    }

    // ── numbers (C4) ────────────────────────────────────────────────────

    /// <summary>
    /// The exact decimal value of a JSON number literal in ES6 Number::toString
    /// layout, computed on digit strings only (see the Python reference for the
    /// rule table). Zero in any spelling is "0".
    /// </summary>
    internal static string CanonicalNumber(string literal)
    {
        if (!NumberRe.IsMatch(literal))
        {
            throw new McpCanonicalizationException("not_json", $"not a JSON number: {literal}");
        }
        bool negative = literal[0] == '-';
        string s = negative ? literal[1..] : literal;
        int expAt = s.IndexOfAny(new[] { 'e', 'E' });
        string mantissa = expAt < 0 ? s : s[..expAt];
        BigInteger exp10 = expAt < 0
            ? BigInteger.Zero
            : BigInteger.Parse(s[(expAt + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        int dot = mantissa.IndexOf('.');
        string intPart = dot < 0 ? mantissa : mantissa[..dot];
        string fracPart = dot < 0 ? string.Empty : mantissa[(dot + 1)..];
        string digits = (intPart + fracPart).TrimStart('0');
        if (digits.Length == 0)
        {
            return "0";
        }
        BigInteger scale = exp10 - fracPart.Length;
        string stripped = digits.TrimEnd('0');
        scale += digits.Length - stripped.Length;
        digits = stripped;
        int k = digits.Length;
        BigInteger n = k + scale;
        string body;
        if (n >= k && n <= 21)
        {
            body = digits + new string('0', (int)(n - k));
        }
        else if (n > 0 && n <= 21)
        {
            int pos = (int)n;
            body = digits[..pos] + "." + digits[pos..];
        }
        else if (n > -6 && n <= 0)
        {
            body = "0." + new string('0', (int)(-n)) + digits;
        }
        else
        {
            BigInteger exponent = n - 1;
            body = digits[0]
                + (k > 1 ? "." + digits[1..] : string.Empty)
                + "e"
                + (exponent.Sign >= 0 ? "+" : "-")
                + BigInteger.Abs(exponent).ToString(CultureInfo.InvariantCulture);
        }
        return negative ? "-" + body : body;
    }

    // ── serialization (C3, C5, C6, C8) ──────────────────────────────────

    private static string CanonicalText(object? tree)
    {
        var sb = new StringBuilder();
        Write(tree, sb, 0);
        return sb.ToString();
    }

    private static void Write(object? value, StringBuilder sb, int depth)
    {
        switch (value)
        {
            case null:
                sb.Append("null");
                break;
            case bool b:
                sb.Append(b ? "true" : "false");
                break;
            case NumberLiteral num:
                sb.Append(CanonicalNumber(num.Literal));
                break;
            case string s:
                Quote(s, sb);
                break;
            case List<object?> list:
                if (depth + 1 > MaxDepth)
                {
                    throw new McpCanonicalizationException("depth_exceeded", $"nesting exceeds {MaxDepth}");
                }
                sb.Append('[');
                for (int i = 0; i < list.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(',');
                    }
                    Write(list[i], sb, depth + 1);
                }
                sb.Append(']');
                break;
            case SortedDictionary<string, object?> obj:
                if (depth + 1 > MaxDepth)
                {
                    throw new McpCanonicalizationException("depth_exceeded", $"nesting exceeds {MaxDepth}");
                }
                sb.Append('{');
                bool first = true;
                foreach (var kv in obj)
                {
                    if (!first)
                    {
                        sb.Append(',');
                    }
                    first = false;
                    Quote(kv.Key, sb);
                    sb.Append(':');
                    Write(kv.Value, sb, depth + 1);
                }
                sb.Append('}');
                break;
            default:
                throw new McpCanonicalizationException("not_json", $"unsupported value {value.GetType()}");
        }
    }

    private static void Quote(string s, StringBuilder sb)
    {
        sb.Append('"');
        foreach (char ch in s)
        {
            switch (ch)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (ch < ' ')
                    {
                        sb.Append("\\u").Append(((int)ch).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        sb.Append(ch);
                    }
                    break;
            }
        }
        sb.Append('"');
    }

    // ── Era-B header reconciliation (C11, §6.7) ─────────────────────────

    private static bool HeaderValueConsistent(object? bodyValue, string headerValue)
    {
        try
        {
            object? parsed = ParseStrict(StrictUtf8.GetBytes(headerValue));
            if (CanonicalText(parsed) == CanonicalText(bodyValue))
            {
                return true;
            }
        }
        catch (McpCanonicalizationException)
        {
            // not JSON text — fall through to the raw-string comparison
        }
        catch (EncoderFallbackException)
        {
            // an unpaired surrogate in the header value is never JSON
        }
        return bodyValue is string s && s == headerValue;
    }

    private static IReadOnlyList<string> ReconcileHeaderParams(object? tree, IReadOnlyDictionary<string, string>? headerParams)
    {
        if (headerParams is null || headerParams.Count == 0)
        {
            return Array.Empty<string>();
        }
        if (tree is not SortedDictionary<string, object?> obj)
        {
            throw new McpCanonicalizationException("body_not_object", "header-mirrored parameters require an object body");
        }
        var names = headerParams.Keys.ToList();
        names.Sort(CodePointComparer.Instance);
        foreach (string name in names)
        {
            if (!obj.TryGetValue(name, out object? bodyValue))
            {
                throw new McpCanonicalizationException("header_param_missing", $"Mcp-Param '{name}' has no body counterpart");
            }
            if (!HeaderValueConsistent(bodyValue, headerParams[name]))
            {
                throw new McpCanonicalizationException("header_param_mismatch", $"Mcp-Param '{name}' disagrees with the body");
            }
        }
        return names;
    }
}
