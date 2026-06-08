namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Shared handshake metadata and validation helpers for built-in cross-process transports.
    /// </summary>
    internal static class CrossProcessHandshake
    {
        internal const string Greeting = "HELLO";

        internal static string BuildHelloLine(CrossProcessProtocolDescriptor protocol) =>
            $"{Greeting} fp={CrossProcessUtils.CurrentFingerprint} tk={protocol.TransportKind} proto={protocol.ProtocolId} ver={protocol.ProtocolVersion}";

        internal static bool TryValidateHelloLine(string helloLine, CrossProcessProtocolDescriptor expectedProtocol, out string error)
        {
            error = string.Empty;

            if (string.IsNullOrWhiteSpace(helloLine))
            {
                error = "The server handshake was empty.";
                return false;
            }

            var parts = helloLine.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length < 5 || !string.Equals(parts[0], Greeting, StringComparison.Ordinal))
            {
                error = "The server handshake did not start with the expected greeting.";
                return false;
            }

            var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 1; i < parts.Length; i++)
            {
                var part = parts[i];
                var separatorIndex = part.IndexOf('=');
                if (separatorIndex <= 0 || separatorIndex == part.Length - 1)
                    continue;

                fields[part[..separatorIndex]] = part[(separatorIndex + 1)..];
            }

            if (!fields.TryGetValue("fp", out var fingerprint) || string.IsNullOrWhiteSpace(fingerprint))
            {
                error = "The server handshake did not include a fingerprint.";
                return false;
            }

            if (!fields.TryGetValue("tk", out var transportToken) || !Enum.TryParse<TransportKind>(transportToken, ignoreCase: true, out var declaredTransportKind))
            {
                error = "The server handshake did not include a valid transport kind.";
                return false;
            }

            if (declaredTransportKind != expectedProtocol.TransportKind)
            {
                error = $"The server declared transport '{declaredTransportKind}', but the client expected '{expectedProtocol.TransportKind}'.";
                return false;
            }

            if (!fields.TryGetValue("proto", out var protocolId) || !string.Equals(protocolId, expectedProtocol.ProtocolId, StringComparison.Ordinal))
            {
                error = $"The server declared protocol '{protocolId ?? "<missing>"}', but the client requires '{expectedProtocol.ProtocolId}'.";
                return false;
            }

            if (!fields.TryGetValue("ver", out var versionToken) || !int.TryParse(versionToken, out var protocolVersion))
            {
                error = "The server handshake did not include a valid protocol version.";
                return false;
            }

            if (protocolVersion != expectedProtocol.ProtocolVersion)
            {
                error = $"The server declared protocol version '{protocolVersion}', but the client requires '{expectedProtocol.ProtocolVersion}'.";
                return false;
            }

            return true;
        }
    }
}
