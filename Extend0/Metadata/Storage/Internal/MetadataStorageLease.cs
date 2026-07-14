using Extend0.Metadata.Diagnostics;
using System.Collections.Concurrent;

namespace Extend0.Metadata.Storage.Internal;

/// <summary>
/// Holds the cooperative, cross-process writer lease for a persistent metadata table.
/// </summary>
/// <remarks>
/// <para>
/// File sharing flags are not a portable ownership primitive: Unix permits unlinking an open
/// mapped file and does not consistently enforce <see cref="FileShare.None"/>. Extend0 therefore
/// coordinates through a stable sidecar file and an OS-enforced exclusive handle or byte-range lock.
/// </para>
/// <para>
/// The in-process registry complements the OS lock because some Unix lock implementations treat
/// locks as process-scoped. The sidecar is intentionally retained after release so deletion and a
/// concurrent opener always coordinate on the same inode.
/// </para>
/// </remarks>
internal sealed class MetadataStorageLease : IDisposable
{
    private static readonly ConcurrentDictionary<string, byte> OwnedPaths = new(GetPathComparer());

    private readonly string _key;
    private readonly bool _usesRegionLock;
    private FileStream? _stream;

    public MetadataStorageLease(string key, FileStream stream, bool usesRegionLock)
    {
        _key = key;
        _stream = stream;
        _usesRegionLock = usesRegionLock;
    }

    internal static MetadataStorageLease Acquire(string tablePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tablePath);

        var fullPath = Path.GetFullPath(tablePath);
        var key = NormalizeKey(fullPath);
        if (!OwnedPaths.TryAdd(key, 0))
        {
            throw new MetadataTableLockedException(
                $"Metadata table is already open in this process. Path='{fullPath}'.",
                new IOException("The cooperative metadata storage lease is already owned."));
        }

        FileStream? stream = null;
        try
        {
            var leasePath = GetLeasePath(fullPath);
            var parent = Path.GetDirectoryName(leasePath);
            if (!string.IsNullOrWhiteSpace(parent))
                Directory.CreateDirectory(parent);

            var usesRegionLock = OperatingSystem.IsLinux();
            stream = new FileStream(
                leasePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                usesRegionLock ? FileShare.ReadWrite | FileShare.Delete : FileShare.None);
            if (usesRegionLock)
                stream.Lock(0, 1);
            return new MetadataStorageLease(key, stream, usesRegionLock);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            stream?.Dispose();
            OwnedPaths.TryRemove(key, out _);
            throw new MetadataTableLockedException(
                $"Metadata table is locked by another owner. Path='{fullPath}'.",
                ex);
        }
        catch
        {
            stream?.Dispose();
            OwnedPaths.TryRemove(key, out _);
            throw;
        }
    }

    internal static bool TryAcquire(string tablePath, out MetadataStorageLease? lease)
    {
        try
        {
            lease = Acquire(tablePath);
            return true;
        }
        catch (MetadataTableLockedException)
        {
            lease = null;
            return false;
        }
    }

    internal static string GetLeasePath(string tablePath) => Path.GetFullPath(tablePath) + ".extend0.lock";

    public void Dispose()
    {
        var stream = Interlocked.Exchange(ref _stream, null);
        if (stream is null)
            return;

        try
        {
            try
            {
                // Linux uses an explicit byte-range lock; Windows and macOS use FileShare.None.
            // Disposing the stream below releases the active ownership primitive on every platform.
            if (_usesRegionLock && OperatingSystem.IsLinux())
                    stream.Unlock(0, 1);
            }
            catch (IOException)
            {
                // Unlock is best-effort during teardown; disposing the stream releases the OS lock.
            }
            catch (UnauthorizedAccessException)
            {
                // Unlock is best-effort during teardown; disposing the stream releases the OS lock.
            }
        }
        finally
        {
            stream.Dispose();
            OwnedPaths.TryRemove(_key, out _);
        }
    }

    private static string NormalizeKey(string fullPath) =>
        OperatingSystem.IsWindows() ? fullPath.ToUpperInvariant() : fullPath;

    private static IEqualityComparer<string> GetPathComparer() =>
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
}
