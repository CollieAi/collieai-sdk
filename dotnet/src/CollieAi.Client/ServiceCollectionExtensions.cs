// DI registration. Placed in the Microsoft.Extensions.DependencyInjection
// namespace by convention so AddCollieAi(...) is discoverable without an extra
// using.

using CollieAi;
using CollieAi.Internal;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers <see cref="ICollieClient"/> in an ASP.NET Core / generic-host container.</summary>
public static class CollieAiServiceCollectionExtensions
{
    /// <summary>The named <see cref="System.Net.Http.HttpClient"/> used by the SDK; attach delegating handlers to it.</summary>
    public const string HttpClientName = "CollieAi.Client";

    /// <summary>
    /// Registers <see cref="CollieAi.ICollieClient"/> (singleton) plus a named
    /// <see cref="System.Net.Http.HttpClient"/> via <c>IHttpClientFactory</c>.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configure">Configures <see cref="CollieAi.CollieClientOptions"/> (ApiKey/BaseUrl/ProjectId, …).</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddCollieAi(this IServiceCollection services, Action<CollieClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<CollieClientOptions>()
            .Configure(configure)
            .Validate(o => !string.IsNullOrEmpty(o.ApiKey), "CollieClientOptions.ApiKey is required.")
            .Validate(o => o.BaseUrl is not null, "CollieClientOptions.BaseUrl is required.");

        services.AddHttpClient(HttpClientName, (sp, http) =>
            {
                var o = sp.GetRequiredService<IOptions<CollieClientOptions>>().Value;
                http.BaseAddress = o.BaseUrl;
                http.Timeout = System.Threading.Timeout.InfiniteTimeSpan; // SDK applies per-request timeouts
                http.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", $"Bearer {o.ApiKey}");
                http.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", Wire.UserAgent);
            })
            .ConfigurePrimaryHttpMessageHandler(sp =>
            {
                var o = sp.GetRequiredService<IOptions<CollieClientOptions>>().Value;
                return new SocketsHttpHandler
                {
                    MaxConnectionsPerServer = o.MaxConnectionsPerServer,
                    PooledConnectionLifetime = TimeSpan.FromMinutes(5),
                };
            });

        services.TryAddSingleton<ICollieClient>(sp =>
        {
            var o = sp.GetRequiredService<IOptions<CollieClientOptions>>().Value;
            var factory = sp.GetRequiredService<IHttpClientFactory>();
            var http = factory.CreateClient(HttpClientName);
            var logger = sp.GetService<ILogger<CollieClient>>();
            // ownsHttp:false — the IHttpClientFactory owns the handler lifetime.
            return new CollieClient(o, http, ownsHttp: false, logger: logger, hooks: null);
        });

        return services;
    }
}
