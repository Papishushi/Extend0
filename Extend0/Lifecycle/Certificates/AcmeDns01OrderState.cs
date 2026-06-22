using System.Text.Json;
using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Certificates;

public sealed record AcmeDns01OrderState(
    string DirectoryUrl,
    string AccountLocation,
    string AccountKeyPem,
    string AccountKeyThumbprint,
    string CertificateKeyPem,
    IReadOnlyList<string> Domains,
    string OrderUrl,
    string FinalizeUrl,
    string? CertificateUrl,
    string OrderStatus,
    IReadOnlyList<AcmeDns01AuthorizationState> Authorizations,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public AcmeDns01OrderState WithOrderStatus(
        string orderStatus,
        string? certificateUrl,
        IReadOnlyList<AcmeDns01AuthorizationState> authorizations) =>
        this with
        {
            OrderStatus = orderStatus,
            CertificateUrl = certificateUrl,
            Authorizations = authorizations,
            UpdatedAtUtc = DateTimeOffset.UtcNow
        };

    public void Save(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var fullPath = Path.GetFullPath(path);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        File.WriteAllText(fullPath, JsonSerializer.Serialize(this, JsonOptions));
    }

    public static AcmeDns01OrderState Load(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var state = JsonSerializer.Deserialize<AcmeDns01OrderState>(File.ReadAllText(path), JsonOptions);
        return state ?? throw new InvalidDataException($"ACME DNS-01 state file '{path}' is empty or invalid.");
    }
}

public sealed record AcmeDns01AuthorizationState(
    string AuthorizationUrl,
    string Identifier,
    bool Wildcard,
    string Status,
    string DnsChallengeUrl,
    string Token,
    string TxtRecordName,
    string TxtRecordValue);

public sealed record AcmeDns01FinalizationResult(
    AcmeDns01OrderState State,
    string CertificateChainPem);

public sealed record AcmeCertificateFiles(
    string CertificateChainPath,
    string PrivateKeyPath,
    string? PfxPath);
