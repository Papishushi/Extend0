using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Represents the DNS-01 proof material required by ACME-compatible certificate authorities.
/// </summary>
public sealed record Dns01Challenge(
    string Domain,
    string AuthorizationDomain,
    string Token,
    string KeyAuthorization,
    string TxtRecordName,
    string TxtRecordValue)
{
    public const string TxtRecordPrefix = "_acme-challenge";

    public static Dns01Challenge Create(string domain, string token, string keyAuthorization)
    {
        var normalizedDomain = NormalizeDomain(domain);
        var authorizationDomain = RemoveWildcardPrefix(normalizedDomain);
        var normalizedToken = NormalizeToken(token);
        var normalizedKeyAuthorization = NormalizeKeyAuthorization(normalizedToken, keyAuthorization);
        var txtRecordValue = ComputeTxtRecordValue(normalizedKeyAuthorization);

        return new Dns01Challenge(
            normalizedDomain,
            authorizationDomain,
            normalizedToken,
            normalizedKeyAuthorization,
            $"{TxtRecordPrefix}.{authorizationDomain}",
            txtRecordValue);
    }

    public static Dns01Challenge CreateFromAccountThumbprint(
        string domain,
        string token,
        string accountKeyThumbprint)
    {
        var normalizedToken = NormalizeToken(token);
        var normalizedThumbprint = NormalizeBase64UrlPart(accountKeyThumbprint, nameof(accountKeyThumbprint));
        return Create(domain, normalizedToken, $"{normalizedToken}.{normalizedThumbprint}");
    }

    public static string ComputeTxtRecordValue(string keyAuthorization)
    {
        var normalized = NormalizeAsciiToken(keyAuthorization, nameof(keyAuthorization));
        var digest = SHA256.HashData(Encoding.ASCII.GetBytes(normalized));
        return Base64Url.Encode(digest);
    }

    private static string NormalizeDomain(string domain)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(domain);

        var trimmed = domain.Trim();
        if (trimmed.Contains("://", StringComparison.Ordinal)
            || trimmed.Contains('/', StringComparison.Ordinal)
            || trimmed.Contains('\\', StringComparison.Ordinal)
            || trimmed.Contains(':', StringComparison.Ordinal))
        {
            throw new ArgumentException("Domain must be a DNS name, not a URL or endpoint.", nameof(domain));
        }

        trimmed = trimmed.TrimEnd('.');
        var wildcard = trimmed.StartsWith("*.", StringComparison.Ordinal);
        if (wildcard)
            trimmed = trimmed[2..];

        if (trimmed.Length == 0)
            throw new ArgumentException("Domain cannot be empty.", nameof(domain));

        var idn = new IdnMapping();
        var labels = trimmed.Split('.', StringSplitOptions.TrimEntries);
        if (labels.Length == 0)
            throw new ArgumentException("Domain must contain at least one DNS label.", nameof(domain));

        var normalizedLabels = new string[labels.Length];
        for (var i = 0; i < labels.Length; i++)
        {
            if (labels[i].Length == 0)
                throw new ArgumentException("Domain cannot contain empty DNS labels.", nameof(domain));

            var label = idn.GetAscii(labels[i]).ToLowerInvariant();
            ValidateDnsLabel(label, domain);
            normalizedLabels[i] = label;
        }

        return wildcard
            ? $"*.{string.Join('.', normalizedLabels)}"
            : string.Join('.', normalizedLabels);
    }

    private static string RemoveWildcardPrefix(string normalizedDomain) =>
        normalizedDomain.StartsWith("*.", StringComparison.Ordinal)
            ? normalizedDomain[2..]
            : normalizedDomain;

    private static void ValidateDnsLabel(string label, string originalDomain)
    {
        if (label.Length is 0 or > 63)
            throw new ArgumentException($"Domain label '{label}' has an invalid DNS length.", nameof(originalDomain));

        if (label[0] == '-' || label[^1] == '-')
            throw new ArgumentException($"Domain label '{label}' cannot start or end with '-'.", nameof(originalDomain));

        foreach (var ch in label)
        {
            if (!char.IsAsciiLetterOrDigit(ch) && ch != '-')
                throw new ArgumentException($"Domain label '{label}' contains an invalid DNS character.", nameof(originalDomain));
        }
    }

    private static string NormalizeToken(string token) =>
        NormalizeBase64UrlPart(token, nameof(token));

    private static string NormalizeKeyAuthorization(string token, string keyAuthorization)
    {
        var normalized = NormalizeAsciiToken(keyAuthorization, nameof(keyAuthorization));
        if (!normalized.StartsWith($"{token}.", StringComparison.Ordinal))
            throw new ArgumentException("Key authorization must start with '<token>.'.", nameof(keyAuthorization));

        return normalized;
    }

    private static string NormalizeBase64UrlPart(string value, string argumentName)
    {
        var normalized = NormalizeAsciiToken(value, argumentName);
        foreach (var ch in normalized)
        {
            if (!char.IsAsciiLetterOrDigit(ch) && ch is not '-' and not '_')
                throw new ArgumentException($"{argumentName} must be an unpadded base64url token.", argumentName);
        }

        return normalized;
    }

    private static string NormalizeAsciiToken(string value, string argumentName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, argumentName);

        var normalized = value.Trim();
        foreach (var ch in normalized)
        {
            if (ch > 0x7F || char.IsWhiteSpace(ch))
                throw new ArgumentException($"{argumentName} must be a single ASCII token without whitespace.", argumentName);
        }

        return normalized;
    }
}
