using System.Security.Cryptography;
using System.Text;

namespace Extend0.Lifecycle.CrossProcess
{
    internal readonly record struct UnixDomainSocketEndpointName(string Path)
    {
        internal static string BuildPath(string baseName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(baseName, nameof(baseName));

            var hash = Convert
                .ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(baseName)))
                .ToLowerInvariant();

            return System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"extend0-{hash[..24]}.sock");
        }

        internal static UnixDomainSocketEndpointName Parse(string endpointName)
        {
            if (string.IsNullOrWhiteSpace(endpointName))
                throw new ArgumentException("Unix domain socket endpoint cannot be empty.", nameof(endpointName));

            var value = endpointName.Trim();
            if (value.StartsWith("unix://", StringComparison.OrdinalIgnoreCase))
                value = Uri.UnescapeDataString(value["unix://".Length..]);

            return new UnixDomainSocketEndpointName(System.IO.Path.GetFullPath(value));
        }
    }
}
