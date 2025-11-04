using Microsoft.Extensions.Logging;
using System.Security.Cryptography;
using System.Text;

namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// Utilities for deriving deterministic, cross-process identifiers (fingerprints and names)
/// for IPC endpoints and OS-level synchronization primitives, scoped to the current build.
/// </summary>
internal static class CrossProcessUtils
{
    /// <summary>
    /// Gets a stable fingerprint for the current assembly based on the module version ID (MVID).
    /// </summary>
    /// <remarks>
    /// The value is the current assembly's MVID encoded as a 32-character hexadecimal string (format "N").
    /// It changes on every rebuild, ensuring isolation between different binary versions.
    /// </remarks>
    public static string CurrentFingerprint
    {
        get
        {
            var mvid = typeof(CrossProcessUtils).Assembly.ManifestModule.ModuleVersionId;
            return mvid.ToString("N");
        }
    }

    /// <summary>
    /// Builds a deterministic base name for a service type <typeparamref name="T"/> combined with the
    /// assembly fingerprint and an optional user-provided suffix.
    /// </summary>
    /// <typeparam name="T">The service/contract type to scope the name to.</typeparam>
    /// <param name="name">
    /// Optional human-friendly suffix to distinguish multiple logical instances
    /// (e.g., per tenant or environment). If <c>null</c> or whitespace, no suffix is appended.
    /// </param>
    /// <returns>
    /// A string of the form <c>"CPS:{FullTypeName}:{MVID}[ :{name} ]"</c>, where <c>MVID</c> is the
    /// 32-hex-character module version ID of the assembly containing <typeparamref name="T"/>.
    /// </returns>
    public static string BuildNameFor<T>(string? name)
    {
        var mvid = typeof(T).Assembly.ManifestModule.ModuleVersionId.ToString("N");
        var type = typeof(T).FullName ?? typeof(T).Name;
        var suffix = string.IsNullOrWhiteSpace(name) ? "" : $":{name}";
        return $"CPS:{type}:{mvid}{suffix}";
    }

    /// <summary>
    /// Creates and owns a named <see cref="Mutex"/> for cross-process coordination.
    /// Prefers the Global\ namespace on Windows; falls back to Local\ (or no prefix on non-Windows)
    /// when access is denied (e.g., missing SeCreateGlobalPrivilege).
    /// </summary>
    /// <param name="baseName">Base name (e.g., from <see cref="BuildNameFor{T}(string)"/>).</param>
    /// <param name="preferGlobal">
    /// If <c>true</c>, first attempt uses <c>Global\</c> on Windows. Ignored on non-Windows.
    /// </param>
    /// <param name="createdNew">
    /// <c>true</c> if the calling code created the named mutex; <c>false</c> if it already existed.
    /// </param>
    /// <param name="isGlobal">
    /// <c>true</c> if the returned mutex uses the <c>Global\</c> namespace on Windows; otherwise <c>false</c>.
    /// On non-Windows platforms this is always <c>false</c>.
    /// </param>
    /// <param name="logger">Optional logger for diagnostic messages.</param>
    /// <returns>
    /// An owned <see cref="Mutex"/> instance. Call <see cref="Mutex.ReleaseMutex"/> when done and dispose it.
    /// </returns>
    /// <remarks>
    /// <para>
    /// On Windows, creating objects under <c>Global\</c> may require the <c>SeCreateGlobalPrivilege</c>.
    /// If the attempt throws <see cref="UnauthorizedAccessException"/>, this method retries under
    /// <c>Local\</c>. On non-Windows platforms, the name is used as-is.
    /// </para>
    /// <para>
    /// This method uses <c>initiallyOwned: true</c> to take ownership immediately if the mutex is created.
    /// If the mutex already exists, the caller does <b>not</b> gain ownership and must use
    /// <see cref="WaitHandle.WaitOne()"/> to acquire it if needed.
    /// </para>
    /// </remarks>
    public static Mutex CreateOwned(string baseName, bool preferGlobal, out bool createdNew, out bool isGlobal, ILogger? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(baseName, nameof(baseName));

        isGlobal = false;

        // Non-Windows: no Global\ / Local\ distinction
        if (!OperatingSystem.IsWindows()) return new Mutex(initiallyOwned: true, baseName, out createdNew);

        // Windows: try Global\ first if requested
        if (preferGlobal)
        {
            var globalName = $@"Global\{baseName}";
            try
            {
                var m = new Mutex(initiallyOwned: true, globalName, out createdNew);
                isGlobal = true;
                return m;
            }
            catch (UnauthorizedAccessException uae)
            {
                logger?.LogInformation(uae,
                    "No permission for Global\\ mutex '{Name}'. Falling back to Local\\.", globalName);
                // fall through to Local\
            }
        }

        // Local\ (or explicitly chosen when preferGlobal == false)
        var localName = $@"Local\{baseName}";
        try
        {
            return new Mutex(initiallyOwned: true, localName, out createdNew);
        }
        catch (UnauthorizedAccessException uae)
        {
            logger?.LogWarning(uae,
                "No permission for Local\\ mutex '{Name}'. Retrying without prefix.", localName);
            // Final fallback: no prefix (session-local by default)
            return new Mutex(initiallyOwned: true, baseName, out createdNew);
        }
    }

    /// <summary>
    /// Builds a cross-platform, named-pipe-safe endpoint name from an arbitrary base name.
    /// </summary>
    /// <param name="baseName">
    /// The arbitrary identifier to encode into the pipe name (converted from UTF-8 bytes).
    /// </param>
    /// <param name="prefix">
    /// Optional prefix to prepend (e.g., <c>"CPS."</c>). If <c>null</c>, nothing is prepended.
    /// </param>
    /// <returns>
    /// A deterministic pipe-safe identifier containing only URL/pipe-friendly characters
    /// (letters, digits, <c>-</c>, <c>_</c>, and <c>.</c>), suitable for
    /// <see cref="System.IO.Pipes.NamedPipeServerStream"/> / <see cref="System.IO.Pipes.NamedPipeClientStream"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// The method encodes <paramref name="baseName"/> as Base64URL (trims trailing <c>'='</c>,
    /// replaces <c>'+'</c> with <c>'-'</c> and <c>'/'</c> with <c>'_'</c>).
    /// </para>
    /// <para>
    /// To keep names short, if the encoded value exceeds a conservative maximum (<c>220</c> characters),
    /// the result is truncated to the first <c>100</c> characters and a dot plus a 16-hex
    /// SHA-256 suffix is appended. This preserves determinism while reducing collision risk and
    /// staying well within typical OS limits for pipe names.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// var pipeName = BuildPipeName("CPS:MyService:abc123"); // e.g., "CPS.LUNTOX..._..."
    /// </code>
    /// </example>
    public static string BuildPipeName(string baseName, string? prefix = "CPS.")
    {
        ArgumentNullException.ThrowIfNull(baseName);

        // Base64URL encode (no '+', '/', or '=')
        var b64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(baseName))
                  .TrimEnd('=').Replace('+', '-').Replace('/', '_');

        // Keep it short; fall back to a hashed suffix if too long
        const int max = 220; // headroom for prefixes
        if (b64.Length <= max) return (prefix ?? string.Empty) + b64;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(baseName))).ToLowerInvariant();
        return $"{prefix ?? string.Empty}{b64[..100]}.{hash[..16]}";
    }

}
