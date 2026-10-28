using System.Reflection;
using Ruvio.Client;

namespace Ruvio.Extensions.Idempotency.Tests;

/// <summary>Builds reply values for scripted clients; the factories are internal to Ruvio.Client, so they are reached by reflection.</summary>
internal static class Resp
{
    private static T Call<T>(string name, params object?[] args)
    {
        MethodInfo method = typeof(RespValue)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .First(m => m.Name == name && m.GetParameters().Length == args.Length
                && m.GetParameters().Select((p, i) => args[i] is null || p.ParameterType.IsInstanceOfType(args[i]) || p.ParameterType.IsAssignableFrom(args[i]!.GetType())).All(x => x));

        return (T)method.Invoke(null, args)!;
    }

    public static RespValue Simple(string text) => Call<RespValue>("Simple", text);

    public static RespValue Ok => Simple("OK");

    public static RespValue Error(string text) => Call<RespValue>("Error", text);

    public static RespValue Null() => Call<RespValue>("Null");

    public static RespValue Int(long value) => Call<RespValue>("FromInteger", value);

    public static RespValue Bulk(string text) => Call<RespValue>("Bulk", System.Text.Encoding.UTF8.GetBytes(text));

    public static RespValue Bulk(byte[] bytes) => Call<RespValue>("Bulk", bytes);

    public static RespValue Array(params RespValue[] items) => Call<RespValue>("Array", (IReadOnlyList<RespValue>)items);
}
