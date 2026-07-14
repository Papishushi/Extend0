using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Extend0.Lifecycle.CrossProcess;

/// <summary>
/// Thread-independent cross-process ownership lease used on Unix platforms.
/// </summary>
internal sealed class CrossProcessFileLease : IDisposable
{
    private static readonly ConcurrentDictionary<string, byte> OwnedNames = new(StringComparer.Ordinal);

    private readonly string _name;
    private readonly bool _usesRegionLock;
    private FileStream? _stream;

    public CrossProcessFileLease(string name, FileStream stream, bool usesRegionLock)
    {
        _name = name;
        _stream = stream;
        _usesRegionLock = usesRegionLock;
    }

    internal static bool TryAcquire(string name, out CrossProcessFileLease? lease)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        lease = null;

        if (!OwnedNames.TryAdd(name, 0))
            return false;

        FileStream? stream = null;
        try
        {
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(name))).ToLowerInvariant();
            var path = Path.Combine(Path.GetTempPath(), $"extend0-cps-{hash[..32]}.lock");
            var usesRegionLock = OperatingSystem.IsLinux();
            stream = new FileStream(
                path,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                usesRegionLock ? FileShare.ReadWrite | FileShare.Delete : FileShare.None);
            if (usesRegionLock)
                stream.Lock(0, 1);

            lease = new CrossProcessFileLease(name, stream, usesRegionLock);
            return true;
        }
        catch (IOException)
        {
            stream?.Dispose();
            OwnedNames.TryRemove(name, out _);
            return false;
        }
        catch
        {
            stream?.Dispose();
            OwnedNames.TryRemove(name, out _);
            throw;
        }
    }

    public void Dispose()
    {
        var stream = Interlocked.Exchange(ref _stream, null);
        if (stream is null)
            return;

        try
        {
            if (_usesRegionLock && OperatingSystem.IsLinux())
            {
                try { stream.Unlock(0, 1); }
                catch (IOException)
                {
                    // Unlock is best-effort during teardown; disposing the stream releases the OS lock.
                }
            }
        }
        finally
        {
            stream.Dispose();
            OwnedNames.TryRemove(_name, out _);
        }
    }
}
