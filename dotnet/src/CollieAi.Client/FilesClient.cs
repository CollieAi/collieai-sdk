// Files (preview): upload a file for analysis, read its metadata, release it
// (POST/GET/DELETE /v1/files). The deployment must have file analysis enabled;
// otherwise every call throws FileAnalysisUnavailableException.

using System.Net.Http.Headers;
using System.Text.Json;
using CollieAi.Internal;

namespace CollieAi;

/// <summary>File operations (<c>collie.Files</c>, preview).</summary>
public interface ICollieFilesClient
{
    /// <summary>
    /// Upload the file at <paramref name="path"/>. <paramref name="retain"/> keeps
    /// the bytes for the upload TTL after the last job releases them; otherwise
    /// they go as soon as no job needs them. Attach the returned
    /// <see cref="FileObject.Id"/> through <c>InputModerationRequest.Attachments</c>.
    /// </summary>
    Task<FileObject> UploadAsync(string path, bool retain = false, CancellationToken cancellationToken = default);

    /// <summary>The file's metadata (<c>State</c>: uploaded, leased, deleted, …).</summary>
    Task<FileObject> GetAsync(string fileId, CancellationToken cancellationToken = default);

    /// <summary>Release the file. Its bytes go once no job needs them; a job that already holds it still finishes.</summary>
    Task DeleteAsync(string fileId, CancellationToken cancellationToken = default);
}

internal sealed class FilesClient : ICollieFilesClient
{
    private static readonly Dictionary<string, string> MediaTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        [".pdf"] = "application/pdf",
        [".txt"] = "text/plain",
        [".csv"] = "text/csv",
        [".png"] = "image/png",
        [".jpg"] = "image/jpeg",
        [".jpeg"] = "image/jpeg",
        [".xlsx"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        [".docx"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    };

    private readonly CollieClient _c;

    public FilesClient(CollieClient c) => _c = c;

    public async Task<FileObject> UploadAsync(string path, bool retain = false, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        byte[] bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        string name = Path.GetFileName(path);
        // Byte-array parts: every part computes its length, so the request
        // carries a Content-Length (the API answers 411 without one).
        using var form = new MultipartFormDataContent();
        var file = new ByteArrayContent(bytes);
        file.Headers.ContentType = new MediaTypeHeaderValue(
            MediaTypes.TryGetValue(Path.GetExtension(name), out var type) ? type : "application/octet-stream");
        form.Add(file, "file", name);
        form.Add(new StringContent(retain ? "true" : "false"), "retain");
        var (status, content) = await _c.RequestRawAsync(HttpMethod.Post, "/v1/files", form, Wire.OriginFiles, cancellationToken)
            .ConfigureAwait(false);
        return ToModel(Parse(content, status), status);
    }

    public async Task<FileObject> GetAsync(string fileId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(fileId);
        var (dto, _) = await _c.RequestAsync<FileObjectDto>(
            HttpMethod.Get, $"/v1/files/{Uri.EscapeDataString(fileId)}", null, Wire.OriginFiles, cancellationToken)
            .ConfigureAwait(false);
        return ToModel(dto, 200);
    }

    public async Task DeleteAsync(string fileId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(fileId);
        await _c.RequestRawAsync(
            HttpMethod.Delete, $"/v1/files/{Uri.EscapeDataString(fileId)}", null, Wire.OriginFiles, cancellationToken)
            .ConfigureAwait(false);
    }

    private static FileObjectDto Parse(string content, int status)
    {
        try
        {
            return JsonSerializer.Deserialize<FileObjectDto>(content, Wire.Json)
                ?? throw new JsonException("null body");
        }
        catch (JsonException ex)
        {
            throw new CollieApiException("API returned a malformed file object", status, "invalid_response", ex);
        }
    }

    private static FileObject ToModel(FileObjectDto d, int status)
    {
        if (string.IsNullOrEmpty(d.Id) || d.Bytes is null || string.IsNullOrEmpty(d.Sha256)
            || string.IsNullOrEmpty(d.State) || d.CreatedAt is null)
            throw new CollieApiException("API returned a malformed file object", status, "invalid_response");
        return new FileObject
        {
            Id = d.Id,
            Bytes = d.Bytes.Value,
            Sha256 = d.Sha256,
            Filename = d.Filename,
            DeclaredMediaType = d.DeclaredMediaType,
            State = d.State,
            Retain = d.Retain,
            CreatedAt = d.CreatedAt.Value,
            ExpiresAt = d.ExpiresAt,
        };
    }
}
