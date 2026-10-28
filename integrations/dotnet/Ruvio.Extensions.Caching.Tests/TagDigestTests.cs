using System.Text;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public sealed class TagDigestTests
{
    // Fixed values: stored entries and tag keys carry these bytes, so a change here breaks entries already written.
    [Theory]
    [InlineData("category:9", "12697f2d57493cf439b62fe6fcf86fb04d9518a51775983ffad2258ec69a1712")]
    [InlineData("ürün/ğ", "546d73427e8ea883c9c81c90e4aafab3f6d8b84f15f1d3e96fb1b9ceb81b19a5")]
    public void TagDigestsKeepTheirStoredForm(string tag, string expected)
    {
        Span<byte> hex = stackalloc byte[64];

        RuvioDistributedCache.Digest(tag, hex);

        Assert.Equal(expected, Encoding.ASCII.GetString(hex));
    }

    [Fact]
    public void ATagLongerThanTheStackBufferHashesTheSame()
    {
        Span<byte> hex = stackalloc byte[64];

        RuvioDistributedCache.Digest(new string('x', 300), hex);

        Assert.Equal("0d4e2ca9e9cbced7a7a5380eb29e1a3783b9b6d0db72de36a1051038e1c1fbc7", Encoding.ASCII.GetString(hex));
    }
}
