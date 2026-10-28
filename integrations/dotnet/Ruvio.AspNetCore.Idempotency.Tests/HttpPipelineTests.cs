using System.Net;
using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public class HttpPipelineTests
{
    [LiveFact]
    public async Task Real_http_uses_native_idem_to_replay_and_reject_changed_requests()
    {
        var endpoint = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        await using var connection = await RuvioConnection.ConnectAsync(endpoint[0], int.Parse(endpoint[1]));
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IRuvioClient>(client);
        builder.Services.AddRuvioIdempotency(o =>
        {
            o.Namespace = "native-http-" + Guid.NewGuid().ToString("N");
            o.ScopeSelector = _ => "tenant";
            o.Retention = TimeSpan.FromSeconds(10);
        });

        await using var app = builder.Build();
        app.Use(async (context, next) =>
        {
            context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "user")], "test"));
            await next();
        });

        app.UseRouting();
        app.UseRuvioIdempotency();
        int calls = 0;

        app.MapPost("/native", () =>
        {
            calls++;

            return new { value = "saved" };
        }).WithMetadata(new RuvioIdempotencyAttribute());

        await app.StartAsync();
        try
        {
            string address = app.Services.GetRequiredService<IServer>().Features
                .Get<IServerAddressesFeature>()!.Addresses.Single();
            using var http = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(10) };

            http.DefaultRequestHeaders.Add("Idempotency-Key", "native-request");
            using var first = await http.PostAsync("/native", new StringContent("{}"));
            using var replay = await http.PostAsync("/native", new StringContent("{}"));

            Assert.Equal(HttpStatusCode.OK, first.StatusCode);
            Assert.Equal(HttpStatusCode.OK, replay.StatusCode);
            Assert.Equal(await first.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
            using var conflict = await http.PostAsync("/native", new StringContent("{\"changed\":true}"));

            Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
            Assert.Equal(1, calls);
        }

        finally { await app.StopAsync(); }
    }

    [LiveFact]
    public async Task A_throwing_handler_frees_the_key_only_when_abort_on_exception_is_on()
    {
        var endpoint = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        await using var connection = await RuvioConnection.ConnectAsync(endpoint[0], int.Parse(endpoint[1]));
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();

        foreach (bool abort in new[] { false, true })
        {
            var builder = WebApplication.CreateBuilder();

            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IRuvioClient>(client);
            builder.Services.AddRuvioIdempotency(o =>
            {
                o.Namespace = "abort-http-" + Guid.NewGuid().ToString("N");
                o.ScopeSelector = _ => "tenant";
                o.Retention = TimeSpan.FromSeconds(10);
                o.AbortOnException = abort;
            });

            await using var app = builder.Build();
            app.Use(async (context, next) =>
            {
                context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "user")], "test"));

                try
                {
                    await next();
                }
                catch (InvalidOperationException)
                {
                    context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                }
            });

            app.UseRouting();
            app.UseRuvioIdempotency();
            int calls = 0;

            app.MapPost("/flaky", () =>
            {
                if (++calls == 1)
                    throw new InvalidOperationException("transient");

                return new { value = "saved" };
            }).WithMetadata(new RuvioIdempotencyAttribute());

            await app.StartAsync();
            try
            {
                string address = app.Services.GetRequiredService<IServer>().Features
                    .Get<IServerAddressesFeature>()!.Addresses.Single();
                using var http = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(10) };

                http.DefaultRequestHeaders.Add("Idempotency-Key", "flaky-request");
                using var failed = await http.PostAsync("/flaky", new StringContent("{}"));
                using var retry = await http.PostAsync("/flaky", new StringContent("{}"));
                using var replay = await http.PostAsync("/flaky", new StringContent("{}"));

                Assert.Equal(HttpStatusCode.InternalServerError, failed.StatusCode);

                if (abort)
                {
                    Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
                    Assert.Equal(HttpStatusCode.OK, replay.StatusCode);
                    Assert.Equal(await retry.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
                    Assert.Equal(2, calls);
                }
                else
                {
                    Assert.Equal(HttpStatusCode.Conflict, retry.StatusCode);
                    Assert.Equal(HttpStatusCode.Conflict, replay.StatusCode);
                    Assert.Equal(1, calls);
                }
            }

            finally { await app.StopAsync(); }
        }
    }

    [LiveFact]
    public async Task Real_http_waits_for_a_running_request_and_replays_its_201_with_location()
    {
        var endpoint = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        await using var connection = await RuvioConnection.ConnectAsync(endpoint[0], int.Parse(endpoint[1]));
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();

        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IRuvioClient>(client);
        builder.Services.AddRuvioIdempotency(o =>
        {
            o.Namespace = "native-wait-" + Guid.NewGuid().ToString("N");
            o.ScopeSelector = _ => "tenant";
            o.Retention = TimeSpan.FromSeconds(10);
            o.MismatchStatusCode = StatusCodes.Status422UnprocessableEntity;
            o.ReplayHeaders.Add("Location");
        });

        await using var app = builder.Build();
        app.Use(async (context, next) =>
        {
            context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "user")], "test"));
            await next();
        });

        app.UseRouting();
        app.UseRuvioIdempotency();
        int calls = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        app.MapPost("/orders", async () =>
        {
            Interlocked.Increment(ref calls);
            entered.TrySetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(20));

            return Results.Created("/orders/7", new { id = 7 });
        }).WithRuvioIdempotency(a => a.PendingWaitMilliseconds = 5000);

        await app.StartAsync();
        try
        {
            string address = app.Services.GetRequiredService<IServer>().Features
                .Get<IServerAddressesFeature>()!.Addresses.Single();
            using var http = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(10) };

            http.DefaultRequestHeaders.Add("Idempotency-Key", "double-click");
            Task<HttpResponseMessage> firstCall = http.PostAsync("/orders", new StringContent("{}"));

            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Task<HttpResponseMessage> secondCall = http.PostAsync("/orders", new StringContent("{}"));

            // The second call may reach the server before or after the release; both replay the first result.
            release.SetResult();
            using var first = await firstCall;
            using var second = await secondCall;

            Assert.Equal(HttpStatusCode.Created, first.StatusCode);
            Assert.Equal(HttpStatusCode.Created, second.StatusCode);
            Assert.Equal("/orders/7", second.Headers.Location?.ToString());
            Assert.Equal("true", second.Headers.GetValues("Idempotent-Replayed").Single());
            Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());
            using var changed = await http.PostAsync("/orders", new StringContent("{\"changed\":true}"));

            Assert.Equal(HttpStatusCode.UnprocessableEntity, changed.StatusCode);
            Assert.Equal(1, calls);
        }

        finally { await app.StopAsync(); }
    }

    [Fact(Timeout = 30000)]
    public async Task RealHttpReplaysJsonAndNeverSendsPartialHandlerFailure()
    {
        var (client, _) = StubClient.Create();
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(client);
        builder.Services.AddRuvioIdempotency(o =>
        {
            o.Namespace = "http-tests";
            o.ScopeSelector = _ => "tenant";
        });

        await using var app = builder.Build();
        int outerCallbacks = 0;

        app.Use(async (context, next) =>
        {
            // Test-only authenticated identity; real applications must use their authentication middleware.
            context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "user")], "test"));
            context.Response.OnStarting(() =>
            {
                context.Response.Headers["X-Outer-Callback"] = Interlocked.Increment(ref outerCallbacks).ToString();

                return Task.CompletedTask;
            });

            await next();
        });

        app.UseRouting();
        app.UseRuvioIdempotency();
        int calls = 0;

        app.MapPost("/json", async c =>
        {
            calls++;
            c.Response.OnStarting(() => { c.Response.Headers.ETag = "\"saved\""; return Task.CompletedTask; });

            await c.Response.WriteAsJsonAsync(new { value = "saved" });
        }).WithMetadata(new RuvioIdempotencyAttribute());

        app.MapPost("/failure", async c =>
        {
            calls++;
            await c.Response.WriteAsync("partial-success");
            await c.Response.StartAsync();
            throw new InvalidOperationException("handler failed after buffered write");
        }).WithMetadata(new RuvioIdempotencyAttribute());

        await app.StartAsync();
        try
        {
            string address = app.Services.GetRequiredService<IServer>().Features
                .Get<IServerAddressesFeature>()!.Addresses.Single();
            using var http = new HttpClient { BaseAddress = new Uri(address) };

            http.DefaultRequestHeaders.Add("Idempotency-Key", "json");
            using var first = await http.PostAsync("/json", new StringContent("{}"));
            using var replay = await http.PostAsync("/json", new StringContent("{}"));

            Assert.Equal(HttpStatusCode.OK, first.StatusCode);
            Assert.Equal(await first.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
            Assert.Equal("\"saved\"", replay.Headers.ETag!.Tag);
            Assert.Equal("1", first.Headers.GetValues("X-Outer-Callback").Single());
            Assert.Equal("2", replay.Headers.GetValues("X-Outer-Callback").Single());
            Assert.Equal(1, calls);
            http.DefaultRequestHeaders.Remove("Idempotency-Key");
            http.DefaultRequestHeaders.Add("Idempotency-Key", "failure");
            using var failed = await http.PostAsync("/failure", new StringContent("{}"));

            Assert.Equal(HttpStatusCode.InternalServerError, failed.StatusCode);
            Assert.DoesNotContain("partial-success", await failed.Content.ReadAsStringAsync());
            using var pending = await http.PostAsync("/failure", new StringContent("{}"));

            Assert.Equal(HttpStatusCode.Conflict, pending.StatusCode);
            Assert.Equal(2, calls);
        }

        finally
        {
            await app.StopAsync();
        }
    }
}
