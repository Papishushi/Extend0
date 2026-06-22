using System.Security.Cryptography;
using System.Text;

namespace Extend0.Lifecycle.CrossProcess
{
    /// <summary>
    /// Shared handshake metadata and validation helpers for built-in cross-process transports.
    /// </summary>
    internal static class CrossProcessHandshake
    {
        internal const string Greeting = "HELLO";
        internal const string AuthGreeting = "AUTH";

        internal static string BuildHelloLine(
            CrossProcessProtocolDescriptor protocol,
            CrossProcessAuthenticationOptions? authentication = null)
        {
            var auth = authentication ?? CrossProcessAuthenticationOptions.None;
            var line = $"{Greeting} fp={CrossProcessUtils.CurrentFingerprint} tk={protocol.TransportKind} proto={protocol.ProtocolId} ver={protocol.ProtocolVersion} auth={auth.Mode}";
            return auth.RequiresClientProof
                ? $"{line} nonce={CreateNonce()}"
                : line;
        }

        internal static bool TryValidateHelloLine(string helloLine, CrossProcessProtocolDescriptor expectedProtocol, out string error)
        {
            return TryValidateHelloLine(helloLine, expectedProtocol, out _, out error);
        }

        internal static bool TryValidateHelloLine(
            string helloLine,
            CrossProcessProtocolDescriptor expectedProtocol,
            out CrossProcessHandshakeHello? hello,
            out string error)
        {
            error = string.Empty;
            hello = null;

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

            var fields = ParseFields(parts, startIndex: 1);

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

            var authenticationMode = AuthenticationMode.None;
            if (fields.TryGetValue("auth", out var authenticationToken)
                && (!Enum.TryParse(authenticationToken, ignoreCase: true, out authenticationMode)
                    || !Enum.IsDefined(authenticationMode)))
            {
                error = "The server handshake did not include a valid authentication mode.";
                return false;
            }

            fields.TryGetValue("nonce", out var nonce);
            if (RequiresNonce(authenticationMode) && string.IsNullOrWhiteSpace(nonce))
            {
                error = $"The server handshake requires {authenticationMode} authentication but did not include a nonce.";
                return false;
            }

            hello = new CrossProcessHandshakeHello(
                helloLine,
                fingerprint,
                declaredTransportKind,
                protocolId,
                protocolVersion,
                authenticationMode,
                nonce);

            return true;
        }

        internal static string? CreateClientAuthenticationLine(
            CrossProcessHandshakeHello hello,
            CrossProcessAuthenticationOptions? authentication)
        {
            var auth = authentication ?? CrossProcessAuthenticationOptions.None;

            if (hello.AuthenticationMode == AuthenticationMode.None)
            {
                if (auth.Mode == AuthenticationMode.None)
                    return null;

                throw new IOException($"The server does not require authentication, but the client was configured for '{auth.Mode}'.");
            }

            if (hello.AuthenticationMode == AuthenticationMode.SharedSecretHmac)
                return CreateSharedSecretHmacAuthenticationLine(hello, auth);

            if (hello.AuthenticationMode == AuthenticationMode.SignedChallenge)
                return CreateSignedChallengeAuthenticationLine(hello, auth);

            throw new NotSupportedException($"Authentication mode '{hello.AuthenticationMode}' is not supported by built-in transports yet.");
        }

        internal static bool TryValidateClientAuthenticationLine(
            string? authenticationLine,
            string rawHelloLine,
            CrossProcessAuthenticationOptions? authentication,
            out string error)
        {
            error = string.Empty;
            var auth = authentication ?? CrossProcessAuthenticationOptions.None;

            if (auth.Mode == AuthenticationMode.None)
                return true;

            if (auth.Mode is not (AuthenticationMode.SharedSecretHmac or AuthenticationMode.SignedChallenge))
            {
                error = $"Authentication mode '{auth.Mode}' is not supported by built-in transports yet.";
                return false;
            }

            if (string.IsNullOrWhiteSpace(authenticationLine))
            {
                error = "The client did not send an authentication proof.";
                return false;
            }

            var parts = authenticationLine.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length < 3 || !string.Equals(parts[0], AuthGreeting, StringComparison.Ordinal))
            {
                error = "The client authentication line did not start with the expected greeting.";
                return false;
            }

            var fields = ParseFields(parts, startIndex: 1);
            if (!fields.TryGetValue("mode", out var modeToken)
                || !Enum.TryParse<AuthenticationMode>(modeToken, ignoreCase: true, out var mode)
                || mode != auth.Mode)
            {
                error = $"The client authentication line did not declare {auth.Mode} mode.";
                return false;
            }

            if (auth.Mode == AuthenticationMode.SignedChallenge)
                return TryValidateSignedChallengeAuthenticationLine(fields, rawHelloLine, auth, out error);

            if (!fields.TryGetValue("proof", out var suppliedProof) || string.IsNullOrWhiteSpace(suppliedProof))
            {
                error = "The client authentication line did not include a proof.";
                return false;
            }

            var expectedProof = ComputeProof(auth.SharedSecret!, rawHelloLine);
            if (!FixedTimeEquals(expectedProof, suppliedProof))
            {
                error = "The client authentication proof was invalid.";
                return false;
            }

            return true;
        }

        private static string CreateSharedSecretHmacAuthenticationLine(
            CrossProcessHandshakeHello hello,
            CrossProcessAuthenticationOptions auth)
        {
            if (auth.Mode != AuthenticationMode.SharedSecretHmac)
                throw new IOException("The server requires shared-secret HMAC authentication, but the client was not configured with that mode.");

            var proof = ComputeProof(auth.SharedSecret!, hello.RawLine);
            return $"{AuthGreeting} mode={AuthenticationMode.SharedSecretHmac} proof={proof}";
        }

        private static string CreateSignedChallengeAuthenticationLine(
            CrossProcessHandshakeHello hello,
            CrossProcessAuthenticationOptions auth)
        {
            if (auth.Mode != AuthenticationMode.SignedChallenge)
                throw new IOException("The server requires signed-challenge authentication, but the client was not configured with that mode.");

            if (auth.SignChallenge is null)
                throw new InvalidOperationException("Signed-challenge client authentication requires a signer.");

            var challenge = Encoding.UTF8.GetBytes(hello.RawLine);
            var signature = auth.SignChallenge(challenge);
            if (signature.Length == 0)
                throw new InvalidOperationException("Signed-challenge signer returned an empty signature.");

            var keyId = string.IsNullOrWhiteSpace(auth.SignedChallengeKeyId)
                ? string.Empty
                : $" kid={auth.SignedChallengeKeyId}";
            var signatureToken = Convert.ToBase64String(signature);
            return $"{AuthGreeting} mode={AuthenticationMode.SignedChallenge}{keyId} alg={auth.SignedChallengeAlgorithm} sig={signatureToken}";
        }

        private static bool TryValidateSignedChallengeAuthenticationLine(
            IReadOnlyDictionary<string, string> fields,
            string rawHelloLine,
            CrossProcessAuthenticationOptions auth,
            out string error)
        {
            error = string.Empty;

            if (auth.VerifyChallengeSignature is null)
            {
                error = "Signed-challenge server authentication requires a verifier.";
                return false;
            }

            if (!fields.TryGetValue("alg", out var algorithm) || string.IsNullOrWhiteSpace(algorithm))
            {
                error = "The client authentication line did not include a signed-challenge algorithm.";
                return false;
            }

            if (!string.Equals(algorithm, auth.SignedChallengeAlgorithm, StringComparison.OrdinalIgnoreCase))
            {
                error = $"The client signed-challenge algorithm '{algorithm}' does not match the expected '{auth.SignedChallengeAlgorithm}'.";
                return false;
            }

            if (!fields.TryGetValue("sig", out var signatureToken) || string.IsNullOrWhiteSpace(signatureToken))
            {
                error = "The client authentication line did not include a signature.";
                return false;
            }

            byte[] signature;
            try
            {
                signature = Convert.FromBase64String(signatureToken);
            }
            catch (FormatException)
            {
                error = "The client authentication signature was not valid base64.";
                return false;
            }

            fields.TryGetValue("kid", out var keyId);
            var challenge = Encoding.UTF8.GetBytes(rawHelloLine);
            if (!auth.VerifyChallengeSignature(keyId, challenge, signature))
            {
                error = "The client authentication signature was invalid.";
                return false;
            }

            return true;
        }

        internal static string BuildAuthenticationOkLine() => $"{AuthGreeting} ok=true";

        internal static string BuildAuthenticationErrorLine() => $"{AuthGreeting} ok=false";

        internal static bool TryValidateAuthenticationOkLine(string? authenticationAck, out string error)
        {
            error = string.Empty;
            if (string.IsNullOrWhiteSpace(authenticationAck))
            {
                error = "The server did not acknowledge authentication.";
                return false;
            }

            var parts = authenticationAck.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length < 2 || !string.Equals(parts[0], AuthGreeting, StringComparison.Ordinal))
            {
                error = "The server authentication acknowledgement was malformed.";
                return false;
            }

            var fields = ParseFields(parts, startIndex: 1);
            if (!fields.TryGetValue("ok", out var ok) || !string.Equals(ok, "true", StringComparison.OrdinalIgnoreCase))
            {
                error = "The server rejected authentication.";
                return false;
            }

            return true;
        }

        private static Dictionary<string, string> ParseFields(string[] parts, int startIndex)
        {
            var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var i = startIndex; i < parts.Length; i++)
            {
                var part = parts[i];
                var separatorIndex = part.IndexOf('=');
                if (separatorIndex <= 0 || separatorIndex == part.Length - 1)
                    continue;

                fields[part[..separatorIndex]] = part[(separatorIndex + 1)..];
            }

            return fields;
        }

        private static string CreateNonce()
        {
            Span<byte> bytes = stackalloc byte[16];
            RandomNumberGenerator.Fill(bytes);
            return Convert.ToHexString(bytes);
        }

        private static bool RequiresNonce(AuthenticationMode authenticationMode) =>
            authenticationMode is AuthenticationMode.SharedSecretHmac or AuthenticationMode.SignedChallenge;

        private static string ComputeProof(string sharedSecret, string rawHelloLine)
        {
            var key = Encoding.UTF8.GetBytes(sharedSecret);
            var payload = Encoding.UTF8.GetBytes(rawHelloLine);
            return Convert.ToHexString(HMACSHA256.HashData(key, payload));
        }

        private static bool FixedTimeEquals(string expectedProof, string suppliedProof)
        {
            var expected = Encoding.ASCII.GetBytes(expectedProof);
            var supplied = Encoding.ASCII.GetBytes(suppliedProof);
            return expected.Length == supplied.Length
                && CryptographicOperations.FixedTimeEquals(expected, supplied);
        }
    }

    internal sealed record CrossProcessHandshakeHello(
        string RawLine,
        string Fingerprint,
        TransportKind TransportKind,
        string ProtocolId,
        int ProtocolVersion,
        AuthenticationMode AuthenticationMode,
        string? Nonce);
}
