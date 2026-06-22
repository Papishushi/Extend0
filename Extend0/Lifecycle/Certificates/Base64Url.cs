using System.Text;

namespace Extend0.Lifecycle.Certificates;

public static class Base64Url
{
    public static string Encode(ReadOnlySpan<byte> bytes) =>
        Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    public static string EncodeString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Encode(Encoding.UTF8.GetBytes(value));
    }
}
