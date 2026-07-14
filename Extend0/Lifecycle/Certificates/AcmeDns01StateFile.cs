using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Extend0.Lifecycle.Certificates;

/// <summary>
/// Saves and loads ACME DNS-01 order state, optionally protecting it with portable passphrase encryption.
/// </summary>
public static class AcmeDns01StateFile
{
    private const string ProtectedFormat = "extend0.acme-dns01-state";
    private const int ProtectedVersion = 1;
    private const int SaltSize = 16;
    private const int NonceSize = 12;
    private const int KeySize = 32;
    private const int TagSize = 16;
    private const int KdfIterations = 310_000;
    private static readonly byte[] AssociatedData = Encoding.UTF8.GetBytes(ProtectedFormat);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Saves an ACME DNS-01 state file using the requested protection mode.
    /// </summary>
    /// <param name="path">Destination state file path.</param>
    /// <param name="state">Order state to persist.</param>
    /// <param name="protection">State protection options, or <see langword="null"/> for unprotected JSON.</param>
    public static void Save(
        string path,
        AcmeDns01OrderState state,
        AcmeDns01StateProtectionOptions? protection = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(state);

        protection ??= AcmeDns01StateProtectionOptions.None;
        if (protection.Kind == AcmeDns01StateProtectionKind.None)
        {
            state.Save(path);
            return;
        }

        if (protection.Kind != AcmeDns01StateProtectionKind.Passphrase)
            throw new NotSupportedException($"Unsupported ACME DNS-01 state protection kind '{protection.Kind}'.");

        if (string.IsNullOrWhiteSpace(protection.Passphrase))
            throw new InvalidOperationException("Passphrase state protection requires a passphrase.");

        var fullPath = Path.GetFullPath(path);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var plaintext = JsonSerializer.SerializeToUtf8Bytes(state, JsonOptions);
        var salt = RandomNumberGenerator.GetBytes(SaltSize);
        var nonce = RandomNumberGenerator.GetBytes(NonceSize);
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[TagSize];
        var key = DeriveKey(protection.Passphrase, salt);

        try
        {
            using var aes = new AesGcm(key, TagSize);
            aes.Encrypt(nonce, plaintext, ciphertext, tag, AssociatedData);

            var envelope = new ProtectedStateEnvelope(
                ProtectedFormat,
                ProtectedVersion,
                "PBKDF2-HMAC-SHA256",
                KdfIterations,
                "AES-256-GCM",
                Convert.ToBase64String(salt),
                Convert.ToBase64String(nonce),
                Convert.ToBase64String(tag),
                Convert.ToBase64String(ciphertext));
            File.WriteAllText(fullPath, JsonSerializer.Serialize(envelope, JsonOptions));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(plaintext);
            CryptographicOperations.ZeroMemory(key);
        }
    }

    /// <summary>
    /// Loads an ACME DNS-01 state file and decrypts it when the file is passphrase-protected.
    /// </summary>
    /// <param name="path">Path to the state file.</param>
    /// <param name="protection">Protection options used to decrypt protected state.</param>
    /// <param name="detectedProtectionKind">Detected protection mode of the file on disk.</param>
    /// <returns>The loaded ACME DNS-01 order state.</returns>
    public static AcmeDns01OrderState Load(
        string path,
        AcmeDns01StateProtectionOptions? protection,
        out AcmeDns01StateProtectionKind detectedProtectionKind)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        protection ??= AcmeDns01StateProtectionOptions.None;
        var text = File.ReadAllText(path);
        using var document = JsonDocument.Parse(text);
        if (!IsProtectedEnvelope(document.RootElement))
        {
            detectedProtectionKind = AcmeDns01StateProtectionKind.None;
            return JsonSerializer.Deserialize<AcmeDns01OrderState>(text, JsonOptions)
                ?? throw new InvalidDataException($"ACME DNS-01 state file '{path}' is empty or invalid.");
        }

        detectedProtectionKind = AcmeDns01StateProtectionKind.Passphrase;
        if (protection.Kind != AcmeDns01StateProtectionKind.Passphrase || string.IsNullOrWhiteSpace(protection.Passphrase))
        {
            throw new InvalidOperationException(
                "ACME DNS-01 state is passphrase-protected. Provide --protect-state passphrase with --state-passphrase or --state-passphrase-env.");
        }

        var envelope = JsonSerializer.Deserialize<ProtectedStateEnvelope>(text, JsonOptions)
            ?? throw new InvalidDataException($"ACME DNS-01 protected state file '{path}' is empty or invalid.");
        ValidateEnvelope(envelope);

        var salt = Convert.FromBase64String(envelope.Salt);
        var nonce = Convert.FromBase64String(envelope.Nonce);
        var tag = Convert.FromBase64String(envelope.Tag);
        var ciphertext = Convert.FromBase64String(envelope.Ciphertext);
        var plaintext = new byte[ciphertext.Length];
        var key = DeriveKey(protection.Passphrase, salt);

        try
        {
            using var aes = new AesGcm(key, TagSize);
            aes.Decrypt(nonce, ciphertext, tag, plaintext, AssociatedData);
            return JsonSerializer.Deserialize<AcmeDns01OrderState>(plaintext, JsonOptions)
                ?? throw new InvalidDataException($"ACME DNS-01 protected state file '{path}' decrypted to invalid state.");
        }
        catch (CryptographicException ex)
        {
            throw new InvalidOperationException("Could not decrypt ACME DNS-01 state. Check the passphrase.", ex);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(plaintext);
            CryptographicOperations.ZeroMemory(key);
        }
    }

    private static bool IsProtectedEnvelope(JsonElement root) =>
        root.ValueKind == JsonValueKind.Object
        && root.TryGetProperty("format", out var format)
        && string.Equals(format.GetString(), ProtectedFormat, StringComparison.Ordinal);

    private static byte[] DeriveKey(string passphrase, byte[] salt) =>
        Rfc2898DeriveBytes.Pbkdf2(
            passphrase,
            salt,
            KdfIterations,
            HashAlgorithmName.SHA256,
            KeySize);

    private static void ValidateEnvelope(ProtectedStateEnvelope envelope)
    {
        if (!string.Equals(envelope.Format, ProtectedFormat, StringComparison.Ordinal))
            throw new InvalidDataException($"Protected state format '{envelope.Format}' is not supported.");
        if (envelope.Version != ProtectedVersion)
            throw new InvalidDataException($"Protected state version '{envelope.Version}' is not supported.");
        if (!string.Equals(envelope.Kdf, "PBKDF2-HMAC-SHA256", StringComparison.Ordinal))
            throw new InvalidDataException($"Protected state KDF '{envelope.Kdf}' is not supported.");
        if (envelope.KdfIterations != KdfIterations)
            throw new InvalidDataException($"Protected state KDF iterations '{envelope.KdfIterations}' are not supported.");
        if (!string.Equals(envelope.Cipher, "AES-256-GCM", StringComparison.Ordinal))
            throw new InvalidDataException($"Protected state cipher '{envelope.Cipher}' is not supported.");
    }

    private sealed record ProtectedStateEnvelope(
        string Format,
        int Version,
        string Kdf,
        int KdfIterations,
        string Cipher,
        string Salt,
        string Nonce,
        string Tag,
        string Ciphertext);
}
