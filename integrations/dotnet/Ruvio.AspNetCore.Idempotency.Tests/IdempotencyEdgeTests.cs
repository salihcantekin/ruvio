using System.Reflection;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public class IdempotencyEdgeTests
{
    public class EdgeClient : DispatchProxy
    {
        public Func<RespValue> Begin { get; set; } = () => Resp.Array(Resp.Bulk("acquired"));
        public Func<bool> Complete { get; set; } = () => true;
        public int Calls { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            Calls++;

            if (targetMethod!.Name == nameof(IRuvioClient.BeginIdempotencyAsync))
            {
                return new ValueTask<RespValue>(Begin());
            }

            if (targetMethod.Name == nameof(IRuvioClient.CompleteIdempotencyAsync))
            {
                return new ValueTask<bool>(Complete());
            }

            throw new NotSupportedException(targetMethod.Name);
        }
    }

    private static (IRuvioClient Client, EdgeClient Edge) NewClient()
    {
        var client = DispatchProxy.Create<IRuvioClient, EdgeClient>();

        return (client, (EdgeClient)(object)client);
    }

    private static IdempotencyOptions Options(Action<IdempotencyOptions>? configure = null)
    {
        var options = new IdempotencyOptions
        {
            Namespace = "edge",
            ScopeSelector = _ => "scope",
            MaxRequestBodyBytes = 32,
            MaxResponseBodyBytes = 32,
        };

        configure?.Invoke(options);

        return options;
    }

    private static DefaultHttpContext Context(IRuvioClient? client = null, string body = "hi", bool authenticated = true, string key = "k1")
    {
        var context = new DefaultHttpContext();

        if (authenticated)
        {
            context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "u1")], "test"));
        }

        context.Request.Method = "POST";
        context.Request.Path = "/x";
        context.Request.Headers["Idempotency-Key"] = key;
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
        context.Response.Body = new MemoryStream();
        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(new RuvioIdempotencyAttribute()), "t"));
        var services = new ServiceCollection();

        if (client is not null)
        {
            services.AddSingleton(client);
        }

        context.RequestServices = services.BuildServiceProvider();

        return context;
    }

    private static IdempotencyMiddleware Middleware(RequestDelegate next, IdempotencyOptions? options = null) =>
        new(next, Microsoft.Extensions.Options.Options.Create(options ?? Options()));

    private static readonly RequestDelegate NeverRuns = _ => throw new InvalidOperationException("must not run");

    [Fact]
    public void OptionsValidityCoversEveryBound()
    {
        Assert.True(Options().IsValid());
        Assert.False(Options(o => o.Namespace = " ").IsValid());
        Assert.False(Options(o => o.Namespace = new string('n', 129)).IsValid());
        Assert.True(Options(o => o.Namespace = new string('n', 128)).IsValid());
        Assert.False(Options(o => o.ScopeSelector = null).IsValid());
        Assert.False(Options(o => o.Retention = TimeSpan.Zero).IsValid());
        Assert.False(Options(o => o.Retention = TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond + 1)).IsValid());
        Assert.False(Options(o => o.Retention = IdempotencyOptions.MaximumRetention + TimeSpan.FromMilliseconds(1)).IsValid());
        Assert.True(Options(o => o.Retention = IdempotencyOptions.MaximumRetention).IsValid());
        Assert.True(Options(o => o.Retention = TimeSpan.FromMilliseconds(1)).IsValid());
        Assert.False(Options(o => o.MaxRequestBodyBytes = 0).IsValid());
        Assert.False(Options(o => o.MaxRequestBodyBytes = IdempotencyOptions.MaximumPayloadBytes + 1).IsValid());
        Assert.False(Options(o => o.MaxResponseBodyBytes = 0).IsValid());
        Assert.True(Options(o => o.MaxResponseBodyBytes = IdempotencyOptions.MaximumPayloadBytes).IsValid());
        Assert.Throws<OptionsValidationException>(() => Middleware(_ => Task.CompletedTask, Options(o => o.Namespace = "")));
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddRuvioIdempotency(null!));
    }

    [Fact(Timeout = 30000)]
    public async Task RegistrationValidatesOnStart()
    {
        var services = new ServiceCollection();

        services.AddRuvioIdempotency(o => o.Namespace = "");
        await using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<IdempotencyOptions>>().Value);
    }

    [Fact(Timeout = 30000)]
    public async Task IdentitySelectorDecidesTheCallerAndAnonymousAccessIsOptIn()
    {
        var (client, edge) = NewClient();
        string? caller = null;
        IdempotencyOptions custom = Options(o => o.IdentitySelector = _ => caller);

        foreach (string? blank in new string?[] { null, "", "  " })
        {
            caller = blank;
            DefaultHttpContext context = Context(client);

            await Middleware(NeverRuns, custom).InvokeAsync(context);
            Assert.Equal(401, context.Response.StatusCode);
        }

        Assert.Equal(0, edge.Calls);
        caller = "service-a";
        DefaultHttpContext ok = Context(client, authenticated: false);
        bool ran = false;

        await Middleware(_ => { ran = true; return Task.CompletedTask; }, custom).InvokeAsync(ok);
        Assert.True(ran);

        DefaultHttpContext anonymousDenied = Context(client, authenticated: false);

        await Middleware(NeverRuns).InvokeAsync(anonymousDenied);
        Assert.Equal(401, anonymousDenied.Response.StatusCode);

        DefaultHttpContext blankSubject = Context(client);

        blankSubject.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", " ")], "test"));
        await Middleware(NeverRuns).InvokeAsync(blankSubject);
        Assert.Equal(401, blankSubject.Response.StatusCode);

        ran = false;
        DefaultHttpContext anonymous = Context(client, authenticated: false);

        await Middleware(_ => { ran = true; return Task.CompletedTask; }, Options(o => o.AllowAnonymous = true)).InvokeAsync(anonymous);
        Assert.True(ran);

        ran = false;
        DefaultHttpContext nameId = Context(client);

        nameId.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, "u2")], "test"));
        await Middleware(_ => { ran = true; return Task.CompletedTask; }).InvokeAsync(nameId);
        Assert.True(ran);
    }

    [Fact(Timeout = 30000)]
    public async Task KeyHeaderAndMetadataLengthAreValidatedBeforeTheStoreIsUsed()
    {
        var (client, edge) = NewClient();

        async Task<int> Status(Action<DefaultHttpContext> configure)
        {
            DefaultHttpContext context = Context(client);

            configure(context);
            await Middleware(NeverRuns).InvokeAsync(context);

            return context.Response.StatusCode;
        }

        Assert.Equal(400, await Status(c => c.Request.Headers.Remove("Idempotency-Key")));
        Assert.Equal(400, await Status(c => c.Request.Headers["Idempotency-Key"] = new Microsoft.Extensions.Primitives.StringValues(["a", "b"])));
        Assert.Equal(400, await Status(c => c.Request.Headers["Idempotency-Key"] = new string('k', 129)));
        Assert.Equal(400, await Status(c => c.Request.Headers["Idempotency-Key"] = "tab\tkey"));
        Assert.Equal(400, await Status(c => c.Request.Headers["Idempotency-Key"] = "del\u007fkey"));
        Assert.Equal(400, await Status(c => c.Request.Path = "/" + new string('p', 8200)));
        Assert.Equal(400, await Status(c => c.Request.QueryString = new QueryString("?" + new string('q', 8200))));
        Assert.Equal(400, await Status(c => c.Request.ContentType = new string('c', 8200)));
        Assert.Equal(413, await Status(c => c.Request.ContentLength = 33));
        Assert.Equal(405, await Status(c => c.Request.Method = "GET"));
        Assert.Equal(0, edge.Calls);

        DefaultHttpContext longScope = Context(client);

        await Middleware(NeverRuns, Options(o => o.ScopeSelector = _ => new string('s', 8200))).InvokeAsync(longScope);
        Assert.Equal(400, longScope.Response.StatusCode);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            DefaultHttpContext cancelled = Context(client);

            cancelled.RequestAborted = new CancellationToken(canceled: true);
            await Middleware(NeverRuns).InvokeAsync(cancelled);
        });
    }

    [Fact(Timeout = 30000)]
    public async Task AResponseThatHasAlreadyStartedOrLacksFeaturesIsRefused()
    {
        var (client, edge) = NewClient();
        DefaultHttpContext started = Context(client);

        started.Features.Set<IHttpResponseFeature>(new StartedResponse());
        await Assert.ThrowsAsync<InvalidOperationException>(() => Middleware(NeverRuns).InvokeAsync(started));

        DefaultHttpContext noBody = Context(client);

        noBody.Features.Set<IHttpResponseBodyFeature>(null);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Middleware(NeverRuns).InvokeAsync(noBody));

        DefaultHttpContext noResponse = Context(client);

        noResponse.Features.Set<IHttpResponseFeature>(null);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Middleware(NeverRuns).InvokeAsync(noResponse));
        Assert.Equal(0, edge.Calls);
    }

    private sealed class StartedResponse : HttpResponseFeature
    {
        public override bool HasStarted => true;
    }

    private sealed class FakeTrailers : IHttpResponseTrailersFeature
    {
        public IHeaderDictionary Trailers { get; set; } = new HeaderDictionary { ["X-Trailer"] = "1" };
    }

    [Fact(Timeout = 30000)]
    public async Task UnstorableResponsesFailInsteadOfBeingRecordedWrongly()
    {
        var (client, edge) = NewClient();

        async Task Fails(RequestDelegate handler)
        {
            DefaultHttpContext context = Context(client, key: Guid.NewGuid().ToString("N"));

            await Assert.ThrowsAsync<InvalidOperationException>(() => Middleware(handler).InvokeAsync(context));
        }

        await Fails(c => { c.Features.Set<IHttpResponseBodyFeature>(new Microsoft.AspNetCore.Http.StreamResponseBodyFeature(new MemoryStream())); return Task.CompletedTask; });
        await Fails(c => { c.Features.Set<IHttpResponseTrailersFeature>(new FakeTrailers()); return Task.CompletedTask; });
        await Fails(c => { c.Response.StatusCode = 199; return Task.CompletedTask; });
        await Fails(c => { c.Response.StatusCode = 600; return Task.CompletedTask; });
        await Fails(async c => { c.Response.StatusCode = 204; await c.Response.Body.WriteAsync("x"u8.ToArray()); });
        await Fails(async c => { c.Response.StatusCode = 304; await c.Response.Body.WriteAsync("x"u8.ToArray()); });
        await Fails(async c => { c.Response.ContentLength = 5; await c.Response.Body.WriteAsync("x"u8.ToArray()); });

        edge.Complete = () => true;
        DefaultHttpContext fine = Context(client, key: "ok-key");

        await Middleware(async c =>
        {
            c.Response.StatusCode = 204;
            c.Response.ContentLength = 0;
            await Task.CompletedTask;
        }).InvokeAsync(fine);
        Assert.Equal(204, fine.Response.StatusCode);

        DefaultHttpContext exactLength = Context(client, key: "ok-key-2");

        await Middleware(async c =>
        {
            c.Response.ContentLength = 1;
            await c.Response.Body.WriteAsync("x"u8.ToArray());
        }).InvokeAsync(exactLength);
        Assert.Equal(1, exactLength.Response.Body.Length);
    }

    [Fact(Timeout = 30000)]
    public async Task HeaderValuesWithLineBreaksCannotBeCaptured()
    {
        var (client, _) = NewClient();

        foreach (string value in new[] { "a\rb", "a\nb" })
        {
            DefaultHttpContext context = Context(client, key: "k-" + (int)value[1]);

            await Assert.ThrowsAsync<IOException>(() => Middleware(c =>
            {
                c.Response.Headers["ETag"] = value;

                return Task.CompletedTask;
            }).InvokeAsync(context));
        }

        DefaultHttpContext many = Context(client, key: "k-many");

        await Assert.ThrowsAsync<IOException>(() => Middleware(c =>
        {
            c.Response.Headers["ETag"] = new string('e', 5000);
            c.Response.Headers["Last-Modified"] = new string('m', 5000);

            return Task.CompletedTask;
        }).InvokeAsync(many));
    }

    private static string Stored(int status = 200, Dictionary<string, string>? headers = null, string? body = "aGk=", bool nullHeaders = false)
    {
        var node = new Dictionary<string, object?>
        {
            ["Status"] = status,
            ["Headers"] = nullHeaders ? null : headers ?? [],
            ["Body"] = body,
        };

        return JsonSerializer.Serialize(node);
    }

    [Theory(Timeout = 30000)]
    [InlineData("null")]
    [InlineData("{\"Status\":99,\"Headers\":{},\"Body\":\"\"}")]
    [InlineData("{\"Status\":600,\"Headers\":{},\"Body\":\"\"}")]
    [InlineData("{\"Status\":200,\"Headers\":{},\"Body\":null}")]
    [InlineData("{\"Status\":200,\"Headers\":null,\"Body\":\"\"}")]
    [InlineData("{\"Status\":200,\"Headers\":{\"Content-Type\":\"a\",\"Content-Language\":\"b\",\"ETag\":\"c\",\"Last-Modified\":\"d\",\"X\":\"e\"},\"Body\":\"\"}")]
    [InlineData("{\"Status\":200,\"Headers\":{\"Set-Cookie\":\"a=b\"},\"Body\":\"\"}")]
    [InlineData("{\"Status\":200,\"Headers\":{\"ETag\":null},\"Body\":\"\"}")]
    [InlineData("{\"Status\":200,\"Headers\":{\"ETag\":\"a\\r\\nSet-Cookie: x\"},\"Body\":\"\"}")]
    [InlineData("{\"Status\":200,\"Headers\":{\"ETag\":\"a\\nb\"},\"Body\":\"\"}")]
    [InlineData("{\"Status\":204,\"Headers\":{},\"Body\":\"aGk=\"}")]
    [InlineData("{\"Status\":304,\"Headers\":{},\"Body\":\"aGk=\"}")]
    public async Task CorruptStoredResultsAreNeverReplayed(string stored)
    {
        var (client, edge) = NewClient();

        edge.Begin = () => Resp.Array(Resp.Bulk("completed"), Resp.Bulk(stored));
        DefaultHttpContext context = Context(client);

        await Assert.ThrowsAnyAsync<Exception>(() => Middleware(NeverRuns).InvokeAsync(context));
        Assert.Equal(0, context.Response.Body.Length);
        Assert.Equal(200, context.Response.StatusCode);
    }

    [Fact(Timeout = 30000)]
    public async Task OversizedOrMalformedStoredBodiesAreNeverReplayed()
    {
        var (client, edge) = NewClient();
        IdempotencyOptions options = Options();

        foreach (string stored in new[]
        {
            Stored(body: new string('A', 48)),
            Stored(body: Convert.ToBase64String(new byte[33])),
            Stored(body: "not base64!!"),
            new string('x', 60_000),
            "{not json",
        })
        {
            edge.Begin = () => Resp.Array(Resp.Bulk("completed"), Resp.Bulk(stored));
            DefaultHttpContext context = Context(client);

            await Assert.ThrowsAnyAsync<Exception>(() => Middleware(NeverRuns, options).InvokeAsync(context));
            Assert.Equal(0, context.Response.Body.Length);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task AValidStoredResultIsReplayedWithOnlyTheAllowedHeaders()
    {
        var (client, edge) = NewClient();
        string stored = Stored(201, new() { ["Content-Type"] = "text/plain", ["etag"] = "\"v\"" }, Convert.ToBase64String("hi"u8.ToArray()));

        edge.Begin = () => Resp.Array(Resp.Bulk("completed"), Resp.Bulk(stored));
        DefaultHttpContext context = Context(client);

        await Middleware(NeverRuns).InvokeAsync(context);
        Assert.Equal(201, context.Response.StatusCode);
        Assert.Equal("text/plain", context.Response.Headers.ContentType);
        Assert.Equal("\"v\"", context.Response.Headers.ETag);
        Assert.Equal(2, context.Response.ContentLength);
        Assert.Equal("hi", Encoding.UTF8.GetString(((MemoryStream)context.Response.Body).ToArray()));

        edge.Begin = () => Resp.Array(Resp.Bulk("pending"));
        DefaultHttpContext pending = Context(client);

        await Middleware(NeverRuns).InvokeAsync(pending);
        Assert.Equal(409, pending.Response.StatusCode);
    }

    [Fact(Timeout = 30000)]
    public async Task TheStoreRejectsRepliesThatAreNotAKnownState()
    {
        var (client, edge) = NewClient();

        async Task<(string, string?)> Acquire() =>
            await IdempotencyStore.AcquireAsync(client, "k", "f", "o", TimeSpan.FromSeconds(1), CancellationToken.None);

        edge.Begin = () => Resp.Null();
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);
        edge.Begin = () => Resp.Int(1);
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);
        edge.Begin = () => Resp.Array();
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);
        edge.Begin = () => Resp.Array(Resp.Bulk("acquired"), Resp.Bulk("x"), Resp.Bulk("y"));
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);
        edge.Begin = () => Resp.Array(Resp.Bulk("mystery"));
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);
        edge.Begin = () => Resp.Array(Resp.Bulk("acquired"), Resp.Bulk("x"));
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);
        edge.Begin = () => Resp.Array(Resp.Bulk("completed"));
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);
        edge.Begin = () => Resp.Array(Resp.Bulk("completed"), Resp.Int(5));
        await Assert.ThrowsAsync<InvalidOperationException>(Acquire);

        foreach (string state in new[] { "acquired", "pending", "conflict" })
        {
            edge.Begin = () => Resp.Array(Resp.Bulk(state));
            Assert.Equal((state, null), await Acquire());
        }

        edge.Begin = () => Resp.Array(Resp.Bulk("completed"), Resp.Bulk("payload"));
        Assert.Equal(("completed", "payload"), await Acquire());

        edge.Complete = () => false;
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            IdempotencyStore.CompleteAsync(client, "k", "f", "o", "r", CancellationToken.None));
        edge.Complete = () => true;
        await IdempotencyStore.CompleteAsync(client, "k", "f", "o", "r", CancellationToken.None);
    }

    [Fact(Timeout = 30000)]
    public async Task RequestBufferEnforcesDeclaredAndActualSizes()
    {
        static MemoryStream Of(int count) => new(Enumerable.Repeat((byte)'a', count).ToArray());

        (byte[] _, int length, int? status) = await RequestBuffer.ReadAsync(Of(5), -1, 10, CancellationToken.None);
        Assert.Equal((0, 400), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(5), 11, 10, CancellationToken.None);
        Assert.Equal((0, 413), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(5), 5, 10, CancellationToken.None);
        Assert.Equal((5, (int?)null), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(8), 5, 10, CancellationToken.None);
        Assert.Equal(400, status);
        (_, length, status) = await RequestBuffer.ReadAsync(Of(3), 5, 10, CancellationToken.None);
        Assert.Equal((3, 400), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(0), 0, 10, CancellationToken.None);
        Assert.Equal((0, (int?)null), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(2), 0, 10, CancellationToken.None);
        Assert.Equal(400, status);
        (_, length, status) = await RequestBuffer.ReadAsync(Of(10), null, 10, CancellationToken.None);
        Assert.Equal((10, (int?)null), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(11), null, 10, CancellationToken.None);
        Assert.Equal(413, status);
        (_, length, status) = await RequestBuffer.ReadAsync(Of(9000), null, 10000, CancellationToken.None);
        Assert.Equal((9000, (int?)null), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(4096), null, 4096, CancellationToken.None);
        Assert.Equal((4096, (int?)null), (length, status));
        (_, length, status) = await RequestBuffer.ReadAsync(Of(4097), null, 4096, CancellationToken.None);
        Assert.Equal(413, status);
        (_, length, status) = await RequestBuffer.ReadAsync(Of(0), null, 10, CancellationToken.None);
        Assert.Equal((0, (int?)null), (length, status));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await RequestBuffer.ReadAsync(new BlockingStream(), null, 10, new CancellationToken(canceled: true)));
    }

    private sealed class BlockingStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);

            return 0;
        }
    }

    [Fact(Timeout = 30000)]
    public async Task BoundedBufferRefusesToGrowPastItsLimitAndStaysFailed()
    {
        var buffer = new BoundedBuffer(4);

        Assert.True(buffer.CanWrite);
        Assert.False(buffer.CanRead);
        Assert.False(buffer.CanSeek);
        buffer.WriteByte(1);
        buffer.Write([2, 3], 0, 2);
        Assert.Equal(3, buffer.Length);
        Assert.Equal(3, buffer.Position);
        Assert.Throws<NotSupportedException>(() => buffer.Position = 0);
        Assert.Throws<NotSupportedException>(() => buffer.Read(new byte[1], 0, 1));
        Assert.Throws<NotSupportedException>(() => buffer.Seek(0, SeekOrigin.Begin));
        Assert.Throws<NotSupportedException>(() => buffer.SetLength(0));
        buffer.Flush();
        await buffer.FlushAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await buffer.FlushAsync(new CancellationToken(true)));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await buffer.WriteAsync(new byte[1], new CancellationToken(true)));
        await buffer.WriteAsync(new byte[] { 9 }, 0, 1, CancellationToken.None);
        Assert.Equal(new byte[] { 1, 2, 3, 9 }, buffer.Written.ToArray());
        Assert.False(buffer.Failed);
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Reserve(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Advance(-1));
        Assert.Throws<IOException>(() => buffer.WriteByte(1));
        Assert.True(buffer.Failed);
        Assert.Throws<IOException>(() => buffer.Write([], 0, 0));
        Assert.Throws<IOException>(() => buffer.Reserve(0));
        Assert.Throws<IOException>(() => buffer.Advance(0));

        var other = new BoundedBuffer(4);

        Assert.Equal(4, other.Reserve(0).Length);
        Assert.Equal(4, other.Reserve(4).Length);
        other.Advance(4);
        Assert.Throws<IOException>(() => other.Reserve(1));
    }

    [Fact(Timeout = 30000)]
    public async Task BoundedResponseBodyBuffersWriterOutputAndFilesWithinTheLimit()
    {
        var body = new BoundedResponseBody(8);

        Assert.Same(body.Buffer, body.Stream);
        Assert.Same(body.Writer, body.Writer);
        Assert.Throws<NotSupportedException>(body.DisableBuffering);
        await body.StartAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await body.StartAsync(new CancellationToken(true)));
        await body.CompleteAsync();

        System.IO.Pipelines.PipeWriter writer = body.Writer;

        writer.GetSpan(2)[0] = (byte)'a';
        writer.Advance(1);
        System.IO.Pipelines.FlushResult flushed = await writer.FlushAsync();
        Assert.False(flushed.IsCanceled);
        writer.CancelPendingFlush();
        Assert.True((await writer.FlushAsync()).IsCanceled);
        Assert.False((await writer.FlushAsync()).IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await writer.FlushAsync(new CancellationToken(true)));
        writer.Complete();
        Assert.Throws<IOException>(() => writer.Complete(new InvalidOperationException("boom")));

        string path = Path.GetTempFileName();

        try
        {
            File.WriteAllBytes(path, "0123456789"u8.ToArray());
            var files = new BoundedResponseBody(8);

            await files.SendFileAsync(path, 2, 4);
            Assert.Equal("2345", Encoding.ASCII.GetString(files.Buffer.Written.Span));
            await files.SendFileAsync(path, 8, null);
            Assert.Equal("234589", Encoding.ASCII.GetString(files.Buffer.Written.Span));
            await files.SendFileAsync(path, 10, null);
            await files.SendFileAsync(path, 0, 0);

            await Assert.ThrowsAsync<IOException>(async () => await files.SendFileAsync(path, 0, 10));
            Assert.True(files.Buffer.Failed);

            var other = new BoundedResponseBody(100);

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await other.SendFileAsync(path, -1, null));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await other.SendFileAsync(path, 11, null));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await other.SendFileAsync(path, 0, -1));
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await other.SendFileAsync(path, 5, 20));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await other.SendFileAsync(path, 0, 1, new CancellationToken(true)));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task BufferedResponseFeatureRunsStartingCallbacksLastInFirstOutAndCopiesBack()
    {
        var inner = new HttpResponseFeature { StatusCode = 200, ReasonPhrase = "OK" };
        int completed = 0;
        var feature = new BufferedResponseFeature(new CompletedProbe(inner, () => completed++), new MemoryStream());
        var order = new List<int>();

        Assert.False(feature.HasStarted);
        Assert.Throws<NotSupportedException>(() => feature.Body = new MemoryStream());
        feature.OnStarting(_ => { order.Add(1); return Task.CompletedTask; }, new object());
        feature.OnStarting(_ => { order.Add(2); return Task.CompletedTask; }, new object());
        feature.OnCompleted(_ => Task.CompletedTask, new object());
        Assert.Equal(1, completed);
        await feature.RunStartingAsync();
        Assert.Equal([2, 1], order);

        feature.StatusCode = 418;
        feature.ReasonPhrase = "teapot";
        feature.Headers["X-A"] = "1";
        inner.Headers["Stale"] = "x";
        feature.CopyToOriginal();
        Assert.Equal(418, inner.StatusCode);
        Assert.Equal("teapot", inner.ReasonPhrase);
        Assert.Equal("1", inner.Headers["X-A"]);
        Assert.False(inner.Headers.ContainsKey("Stale"));
    }

    // The probe has to implement the obsolete Body member to satisfy the interface.
#pragma warning disable CS0618
    private sealed class CompletedProbe(IHttpResponseFeature inner, Action onCompleted) : IHttpResponseFeature
    {
        public int StatusCode { get => inner.StatusCode; set => inner.StatusCode = value; }
        public string? ReasonPhrase { get => inner.ReasonPhrase; set => inner.ReasonPhrase = value; }
        public IHeaderDictionary Headers { get => inner.Headers; set => inner.Headers = value; }
        public Stream Body { get => inner.Body; set => inner.Body = value; }
        public bool HasStarted => inner.HasStarted;
        public void OnStarting(Func<object, Task> callback, object state) => inner.OnStarting(callback, state);
        public void OnCompleted(Func<object, Task> callback, object state) { onCompleted(); inner.OnCompleted(callback, state); }
    }
#pragma warning restore CS0618
}
