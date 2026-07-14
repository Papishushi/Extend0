using System.Text;

namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Provides unpadded base64url encoding helpers used by ACME JWS and DNS-01 proof material.
/// </summary>
public static class Base64Url
{
    /// <summary>
    /// Encodes bytes using unpadded base64url.
    /// </summary>
    /// <param name="bytes">Bytes to encode.</param>
    /// <returns>The unpadded base64url representation.</returns>
    public static string Encode(ReadOnlySpan<byte> bytes) =>
        Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    /// <summary>
    /// Encodes a UTF-8 string using unpadded base64url.
    /// </summary>
    /// <param name="value">String value to encode as UTF-8.</param>
    /// <returns>The unpadded base64url representation.</returns>
    public static string EncodeString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Encode(Encoding.UTF8.GetBytes(value));
    }
}
