namespace Extend0.Lifecycle.CrossProcess
{
    internal readonly record struct TcpSocketEndpoint(string Host, int Port)
    {
        internal static TcpSocketEndpoint Parse(string endpointName, string? fallbackHost = null)
        {
            if (string.IsNullOrWhiteSpace(endpointName))
                throw new ArgumentException("TCP endpoint cannot be empty.", nameof(endpointName));

            var value = endpointName.Trim();
            string host;
            string portToken;

            if (value.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
            {
                var uri = new Uri(value, UriKind.Absolute);
                host = uri.Host;
                portToken = uri.Port.ToString();
            }
            else
            {
                var separator = value.LastIndexOf(':');
                if (separator >= 0)
                {
                    host = value[..separator].Trim();
                    portToken = value[(separator + 1)..].Trim();
                }
                else
                {
                    host = string.Empty;
                    portToken = value;
                }
            }

            if (host.Length >= 2 && host[0] == '[' && host[^1] == ']')
                host = host[1..^1];

            host = NormalizeHost(string.IsNullOrWhiteSpace(host) ? fallbackHost : host);
            if (!int.TryParse(portToken, out var port) || port is < 1 or > 65535)
                throw new FormatException($"TCP endpoint '{endpointName}' must include a valid port between 1 and 65535.");

            return new TcpSocketEndpoint(host, port);
        }

        private static string NormalizeHost(string? host)
        {
            if (string.IsNullOrWhiteSpace(host) || string.Equals(host, ".", StringComparison.Ordinal))
                return "127.0.0.1";

            return host.Trim();
        }
    }
}
