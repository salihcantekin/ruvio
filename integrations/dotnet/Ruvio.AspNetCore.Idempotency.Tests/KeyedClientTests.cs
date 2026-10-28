using System.Reflection;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public class KeyedClientTests
{
    [Fact(Timeout = 30000)]
    public async Task ClientKeySelectsTheKeyedClientPerRequest()
    {
        var keyed = DispatchProxy.Create<IRuvioClient, IdempotencyEdgeTests.EdgeClient>();
        var services = new ServiceCollection();

        services.AddKeyedSingleton("idem", keyed);
        var context = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "u1")], "test")),
            RequestServices = services.BuildServiceProvider(),
        };

        context.Request.Method = "POST";
        context.Request.Path = "/x";
        context.Request.Headers["Idempotency-Key"] = "k1";
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("hi"));
        context.Response.Body = new MemoryStream();
        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(new RuvioIdempotencyAttribute()), "t"));
        var options = new IdempotencyOptions { Namespace = "keyed", ScopeSelector = _ => "scope", ClientKey = "idem" };
        var middleware = new IdempotencyMiddleware(_ => Task.CompletedTask, Microsoft.Extensions.Options.Options.Create(options));

        await middleware.InvokeAsync(context);

        Assert.Equal(2, ((IdempotencyEdgeTests.EdgeClient)(object)keyed).Calls);
    }
}
