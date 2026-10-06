using System.Text;

namespace CollieAi.Internal;

/// <summary>
/// SDK-boundary validation for the opaque <c>conversation_id</c> /
/// <c>correlation_id</c> metadata. Mirrors the backend contract: empty is unset,
/// over-length (in Unicode code points) and control characters are rejected
/// before send — never silently truncated. The backend caps both at 128 code
/// points (METADATA_MAX_CHARS).
/// </summary>
internal static class Metadata
{
    public const int MaxCodePoints = 128;

    public static string? Normalize(string? value, string fieldName)
    {
        if (string.IsNullOrEmpty(value))
            return null; // "" == unset at the SDK boundary; omitted from the request

        int count = 0;
        foreach (var rune in value.EnumerateRunes())
        {
            if (Rune.IsControl(rune))
                throw new ArgumentException($"{fieldName} must not contain control characters.", fieldName);
            if (++count > MaxCodePoints)
                throw new ArgumentException(
                    $"{fieldName} must be at most {MaxCodePoints} Unicode code points (rejected, never truncated).",
                    fieldName);
        }
        return value;
    }
}
