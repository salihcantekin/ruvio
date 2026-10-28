using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

public sealed class KeyFormatTests
{
    private static string Hex(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    [Theory]
    [InlineData("/orders?page=1")]
    [InlineData("sipariş-🚀")]
    public void KeysKeepTheDocumentedHashFormat(string value)
    {
        var store = new RuvioOutputCacheStore(
            () => throw new InvalidOperationException("No client needed."),
            Options.Create(new RuvioOutputCacheOptions { InstanceName = "app-ç", MaxKeyBytes = 8192 }));
        string prefix = "ruvio:output:v1:{" + Hex("app-ç") + "}:";
        string large = new('ş', 3000);

        Assert.Equal(prefix + "entry:" + Hex(value), store.EntryKey(value));
        Assert.Equal(prefix + "tag:" + Hex(value), store.TagKey(value));
        Assert.Equal(prefix + "tag:" + Hex(large), store.TagKey(large));
    }

    // Fixed values: entries already in Ruvio are found by these keys, so a change here orphans them.
    [Fact]
    public void KeysMatchFixedGoldenValues()
    {
        var store = new RuvioOutputCacheStore(
            () => throw new InvalidOperationException("No client needed."),
            Options.Create(new RuvioOutputCacheOptions { InstanceName = "app" }));
        const string prefix = "ruvio:output:v1:{A172CEDCAE47474B615C54D510A5D84A8DEA3032E958587430B413538BE3F333}:";

        Assert.Equal(prefix + "entry:97CC274B14581C817ACA9BC6969810BCF9C7A4B8DC127DBB0CFF58BCC59E8541", store.EntryKey("/orders?page=1"));
        Assert.Equal(prefix + "tag:1CC40F4FD022A3F483F67F2DB687ADC4EF59DEC44C5A59DA1D1CA1BA2C4BD874", store.TagKey("product:42"));
    }
}
