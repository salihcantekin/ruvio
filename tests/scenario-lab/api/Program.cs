using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.OutputCaching;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Ruvio.AspNetCore.Idempotency;
using Ruvio.AspNetCore.OutputCaching;
using Ruvio.AspNetCore.RateLimiting;
using Ruvio.Client.AspNetCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);
ProcessMetrics.Register();
var ruvioHost = Environment.GetEnvironmentVariable("RUVIO_HOST") ?? "127.0.0.1";
var ruvioPort = int.TryParse(Environment.GetEnvironmentVariable("RUVIO_PORT"), out var port)
    ? port
    : 6379;

builder.Services.AddRuvioClient(options =>
{
    options.Host = ruvioHost;
    options.Port = ruvioPort;
    options.Password = Environment.GetEnvironmentVariable("RUVIO_PASSWORD");
});
var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "redis";
var redisPassword = Environment.GetEnvironmentVariable("REDIS_PASSWORD") ??
    Environment.GetEnvironmentVariable("RUVIO_PASSWORD");
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect($"{redisHost}:6379,password={redisPassword},abortConnect=false"));
builder.Services.AddSingleton<IScenarioStorage, ScenarioStorage>();

builder.Services.AddRuvioOutputCache(options =>
{
    options.InstanceName = "scenario-lab-catalog-v1";
});

builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "scenario-lab-orders-v1";
    options.ScopeSelector = _ => "scenario-lab";
    options.IdentitySelector = _ => "k6-load-client";
    options.Retention = TimeSpan.FromMinutes(10);
    options.EnableInstrumentation = true;
});

builder.Services.AddRuvioRateLimiting(options =>
{
    options.KeyPrefix = "scenario-lab:rate:v1:";
    options.PartitionKey = context =>
    {
        var runId = context.Request.Headers["X-Lab-Run"].ToString();

        if (runId.Length is < 1 or > 64 ||
            runId.Any(character => !char.IsAsciiLetterOrDigit(character) && character != '-'))
        {
            throw new InvalidOperationException("A bounded X-Lab-Run identifier is required.");
        }

        return runId;
    };
    options.AddPolicy("probe", 20, TimeSpan.FromSeconds(60));
});

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddMeter("Ruvio.AspNetCore.Idempotency")
        .AddMeter("Ruvio.AspNetCore.RateLimiting")
        .AddMeter("Ruvio.AspNetCore.OutputCaching")
        .AddMeter("Ruvio.Client")
        .AddMeter("Ruvio.ScenarioLab")
        .AddView(
            "scenario_storage_duration_milliseconds",
            new ExplicitBucketHistogramConfiguration
            {
                Boundaries =
                [
                    0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 7.5, 10,
                    15, 25, 50, 75, 100, 250, 500, 750, 1_000, 2_500, 5_000
                ]
            })
        .AddView(
            "ruvio_client_socket_phase_duration_milliseconds",
            new ExplicitBucketHistogramConfiguration
            {
                Boundaries =
                [
                    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5,
                    7.5, 10, 15, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000
                ]
            })
        .AddOtlpExporter())
    .WithTracing(tracing => tracing
        .SetSampler(new AlwaysOnSampler())
        .AddAspNetCoreInstrumentation(options =>
        {
            options.EnrichWithHttpRequest = (activity, request) =>
            {
                if (request.Path.StartsWithSegments("/bench"))
                {
                    activity.SetTag("scenario.request_id", request.Headers["X-Lab-Request-ID"].ToString());
                    activity.SetTag("scenario.backend", request.Headers["X-Lab-Backend"].ToString());
                }
            };
        })
        .AddSource(
            "Ruvio.Client",
            "Ruvio.ScenarioLab.Storage",
            "Ruvio.AspNetCore.Idempotency",
            "Ruvio.AspNetCore.RateLimiting")
        .AddOtlpExporter());

builder.Services.AddSingleton<ScenarioState>();

var app = builder.Build();
app.Use(async (context, next) =>
{
    if (!context.Request.Path.StartsWithSegments("/bench"))
    {
        await next(context);

        return;
    }

    string backend = context.Request.Headers["X-Lab-Backend"].ToString();
    string requestId = context.Request.Headers["X-Lab-Request-ID"].ToString();
    string testRun = context.Request.Headers["X-Lab-Test-Run"].ToString();
    string loadStage = context.Request.Headers["X-Lab-Stage"].ToString();

    if (backend is not ("ruvio" or "redis") || requestId.Length is < 1 or > 128 ||
        testRun.Length is < 1 or > 64 || loadStage != "steady")
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;

        return;
    }

    var previous = ScenarioRequestContext.Current;
    ScenarioRequestContext.Current = context;
    context.Items["scenario.backend"] = backend;
    context.Items["scenario.test_run"] = testRun;
    context.Items["scenario.load_stage"] = loadStage;
    Activity.Current?.SetTag("scenario.request_id", requestId);
    Activity.Current?.SetTag("scenario.backend", backend);
    Activity.Current?.SetTag("test_run", testRun);
    Activity.Current?.SetTag("load_stage", loadStage);

    if (Activity.Current is { } requestActivity)
        context.Response.Headers["X-Lab-Trace-ID"] = requestActivity.TraceId.ToString();

    try
    {
        await next(context);
    }
    finally
    {
        ScenarioRequestContext.Current = previous;
    }
});
app.UseRouting();
app.UseRuvioRateLimiting();
app.UseRuvioIdempotency();
app.UseOutputCache();

app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));
app.MapGet("/health/ready", async (Ruvio.Client.IRuvioClient client, CancellationToken cancellationToken) =>
{
    var pong = await client.PingAsync(cancellationToken);

    return pong == "PONG"
        ? Results.Ok(new { status = "ready", ruvio = pong })
        : Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
});

app.MapGet(
    "/internal/metrics/ruvio",
    async (Ruvio.Client.IRuvioClient client, CancellationToken cancellationToken) =>
        Results.Text(
            await client.GetInfoAsync("prometheus", cancellationToken),
            "text/plain; version=0.0.4; charset=utf-8"));

app.MapGet(
    "/internal/metrics/redis",
    async (IConnectionMultiplexer multiplexer) =>
        Results.Text(
            await RedisMetrics.RenderAsync(multiplexer),
            "text/plain; version=0.0.4; charset=utf-8"));

app.MapGet("/api/catalog/{sku}", (string sku, ScenarioState state) =>
{
    state.RecordCatalogOriginRead();

    return Results.Ok(new CatalogItem(sku, "Scenario Lab Widget", DateTimeOffset.UtcNow));
})
.CacheOutput(policy => policy.Expire(TimeSpan.FromSeconds(15)).Tag("catalog"));

app.MapPost("/api/catalog/invalidate", async (IOutputCacheStore store, CancellationToken cancellationToken) =>
{
    await store.EvictByTagAsync("catalog", cancellationToken);

    return Results.NoContent();
});

app.MapPost("/api/orders", (OrderRequest request, ScenarioState state) =>
{
    var order = new CreatedOrder(
        Interlocked.Increment(ref state.OrderSequence),
        request.Sku,
        request.Quantity);

    state.RecordOrderEffect();

    return Results.Created($"/api/orders/{order.OrderNumber}", order);
})
.WithMetadata(
    new RuvioIdempotencyAttribute());

app.MapGet("/api/rate-limited", (ScenarioState state) =>
{
    state.RecordRateLimitedEndpointCall();

    return Results.Ok(new { accepted = true });
})
.WithRuvioRateLimit("probe");

app.MapGet("/bench/catalog/{sku}", async (
    string sku,
    HttpContext context,
    IScenarioStorage storage,
    CancellationToken cancellationToken) =>
{
    string key = $"scenario:bench:cache:{sku}";
    byte[]? cached = await storage.GetCacheAsync(key, cancellationToken);

    if (cached is not null)
    {
        context.Response.Headers["X-Lab-Cache"] = "hit";

        return Results.Bytes(cached, "application/json");
    }

    context.Response.Headers["X-Lab-Cache"] = "miss";
    byte[] value = JsonSerializer.SerializeToUtf8Bytes(
        new CatalogItem(sku, "Scenario Lab Widget", DateTimeOffset.UtcNow));
    await storage.SetCacheAsync(key, value, TimeSpan.FromSeconds(15), cancellationToken);

    return Results.Bytes(value, "application/json");
});

app.MapPost("/bench/orders", async (
    OrderRequest request,
    HttpContext context,
    IScenarioStorage storage,
    ScenarioState state,
    CancellationToken cancellationToken) =>
{
    string key = context.Request.Headers["Idempotency-Key"].ToString();

    if (key.Length is < 1 or > 128)
        return Results.BadRequest();

    string fingerprint = Convert.ToHexString(SHA256.HashData(
        JsonSerializer.SerializeToUtf8Bytes(request)));
    string owner = Guid.NewGuid().ToString("N");
    var result = await storage.BeginIdempotencyAsync(
        $"scenario:bench:idem:{key}", fingerprint, owner, TimeSpan.FromMinutes(10), cancellationToken);

    if (result.State == "completed" && result.Result is not null)
        return Results.Text(result.Result, "application/json", Encoding.UTF8, StatusCodes.Status201Created);

    if (result.State == "conflict")
        return Results.Conflict();

    if (result.State == "pending")
        return Results.StatusCode(425);

    if (result.State != "acquired")
        return Results.StatusCode(StatusCodes.Status500InternalServerError);

    var order = new CreatedOrder(
        Interlocked.Increment(ref state.OrderSequence), request.Sku, request.Quantity);
    string serialized = JsonSerializer.Serialize(order);
    await storage.CompleteIdempotencyAsync(
        $"scenario:bench:idem:{key}", fingerprint, owner, serialized, cancellationToken);
    state.RecordOrderEffect();

    return Results.Text(serialized, "application/json", Encoding.UTF8, StatusCodes.Status201Created);
});

app.MapGet("/bench/rate-limited", async (
    HttpContext context,
    IScenarioStorage storage,
    ScenarioState state,
    CancellationToken cancellationToken) =>
{
    string partition = context.Request.Headers["X-Lab-Run"].ToString();

    if (partition.Length is < 1 or > 128)
        return Results.BadRequest();

    long? remaining = await storage.LimitAsync(
        $"scenario:bench:rate:{partition}", 20, TimeSpan.FromSeconds(60), cancellationToken);

    if (remaining is null)
        return Results.StatusCode(StatusCodes.Status429TooManyRequests);

    state.RecordRateLimitedEndpointCall();

    return Results.Ok(new { accepted = true, remaining });
});

app.MapGet("/_lab/state", (ScenarioState state) => Results.Ok(new
{
    orderEffects = state.OrderEffectCount,
    catalogOriginReads = state.CatalogOriginReadCount,
    rateLimitedEndpointCalls = state.RateLimitedEndpointCallCount
}));

await app.RunAsync();

internal sealed record CatalogItem(string Sku, string Name, DateTimeOffset GeneratedAt);

internal sealed record OrderRequest(string Sku, int Quantity);

internal sealed record CreatedOrder(long OrderNumber, string Sku, int Quantity);

internal sealed class ScenarioState
{
    private static readonly Meter Meter = new("Ruvio.ScenarioLab");
    private readonly Counter<long> orderEffects = Meter.CreateCounter<long>("scenario_lab_order_effects_total");
    private readonly Counter<long> catalogOriginReads = Meter.CreateCounter<long>("scenario_lab_catalog_origin_reads_total");
    private readonly Counter<long> rateLimitedEndpointCalls = Meter.CreateCounter<long>("scenario_lab_rate_endpoint_calls_total");

    public long OrderSequence;
    public long OrderEffectCount;
    public long CatalogOriginReadCount;
    public long RateLimitedEndpointCallCount;

    public void RecordOrderEffect()
    {
        Interlocked.Increment(ref OrderEffectCount);
        orderEffects.Add(1);
    }

    public void RecordCatalogOriginRead()
    {
        Interlocked.Increment(ref CatalogOriginReadCount);
        catalogOriginReads.Add(1);
    }

    public void RecordRateLimitedEndpointCall()
    {
        Interlocked.Increment(ref RateLimitedEndpointCallCount);
        rateLimitedEndpointCalls.Add(1);
    }
}

internal static class ProcessMetrics
{
    private static readonly Meter Meter = new("Ruvio.ScenarioLab");
    private static readonly Process CurrentProcess = Process.GetCurrentProcess();
    private static readonly ObservableGauge<long> WorkingSet = Meter.CreateObservableGauge(
        "api_process_working_set_bytes",
        () =>
        {
            CurrentProcess.Refresh();

            return CurrentProcess.WorkingSet64;
        },
        "By");
    private static readonly ObservableGauge<double> CpuTime = Meter.CreateObservableGauge(
        "api_process_cpu_time_seconds",
        () =>
        {
            CurrentProcess.Refresh();

            return CurrentProcess.TotalProcessorTime.TotalSeconds;
        },
        "s");

    public static void Register()
    {
        _ = WorkingSet;
        _ = CpuTime;
    }
}
