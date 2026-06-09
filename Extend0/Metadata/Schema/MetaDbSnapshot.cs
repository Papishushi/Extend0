using System.Text;
using System.Text.Json;

namespace Extend0.Metadata.Schema;

/// <summary>
/// A file captured inside a MetaDB snapshot.
/// </summary>
public sealed record MetaDbSnapshotFile(
    string RelativePath,
    string Role,
    long Length);

/// <summary>
/// Manifest persisted with every MetaDB snapshot.
/// </summary>
public sealed record MetaDbSnapshotManifest(
    int FormatVersion,
    string? Label,
    DateTimeOffset CreatedAtUtc,
    TableSpec OriginalSpec,
    TableStorageOptions Storage,
    MetaDbSnapshotFile[] Files)
{
    public bool ContainsRuntimeStorage =>
        Files.Any(static file => file.Role is "single-file-map" or "chunked-manifest" or "chunk");
}

/// <summary>
/// File-system snapshot and restore helpers for MetaDB table specs and their current storage layout.
/// </summary>
public static class MetaDbSnapshot
{
    public const int CurrentFormatVersion = 1;

    private const string ManifestFileName = "snapshot.json";
    private const string SpecFileName = "tablespec.json";
    private const string StorageDirectoryName = "storage";
    private const string SingleFileMapFileName = "table.map";

    private static readonly JsonSerializerOptions Json = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// Creates a snapshot directory containing the normalized <see cref="TableSpec"/> and any materialized runtime files.
    /// </summary>
    public static MetaDbSnapshotManifest Create(
        TableSpec spec,
        string snapshotDirectory,
        string? label = null,
        bool overwrite = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotDirectory);

        spec = spec.NormalizeForPersistence();
        spec.Validate();

        var fullSnapshotDirectory = Path.GetFullPath(snapshotDirectory);
        PrepareSnapshotDirectory(fullSnapshotDirectory, overwrite);

        var files = new List<MetaDbSnapshotFile>();

        var snapshotSpecPath = Path.Combine(fullSnapshotDirectory, SpecFileName);
        spec.SaveToFile(snapshotSpecPath, overwrite: true);
        files.Add(DescribeSnapshotFile(fullSnapshotDirectory, snapshotSpecPath, "table-spec"));

        var storage = spec.Storage.Normalize();
        if (storage.Layout == TableStorageLayout.SingleFile)
            CaptureSingleFileStorage(spec, fullSnapshotDirectory, files);
        else if (storage.Layout == TableStorageLayout.Chunked)
            CaptureChunkedStorage(spec, fullSnapshotDirectory, files);
        else
            throw new ArgumentOutOfRangeException(nameof(spec), storage.Layout, "Unknown MetaDB table storage layout.");

        var manifest = new MetaDbSnapshotManifest(
            CurrentFormatVersion,
            label,
            DateTimeOffset.UtcNow,
            spec,
            storage,
            files.ToArray());

        WriteManifest(fullSnapshotDirectory, manifest);
        return manifest;
    }

    /// <summary>
    /// Loads a snapshot manifest from <paramref name="snapshotDirectory"/>.
    /// </summary>
    public static MetaDbSnapshotManifest LoadManifest(string snapshotDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotDirectory);

        var manifestPath = Path.Combine(Path.GetFullPath(snapshotDirectory), ManifestFileName);
        if (!File.Exists(manifestPath))
            throw new FileNotFoundException("MetaDB snapshot manifest not found.", manifestPath);

        var json = File.ReadAllText(manifestPath, Encoding.UTF8);
        var manifest = JsonSerializer.Deserialize<MetaDbSnapshotManifest>(json, Json)
                       ?? throw new InvalidDataException("Invalid MetaDB snapshot manifest.");

        if (manifest.FormatVersion != CurrentFormatVersion)
            throw new InvalidDataException($"Unsupported MetaDB snapshot format version: {manifest.FormatVersion}.");

        manifest.OriginalSpec.Validate();
        return manifest;
    }

    /// <summary>
    /// Restores a snapshot to a new map path or chunked table directory and returns the relocated spec.
    /// </summary>
    public static TableSpec Restore(
        string snapshotDirectory,
        string restoreMapPath,
        bool overwrite = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotDirectory);
        ArgumentException.ThrowIfNullOrWhiteSpace(restoreMapPath);

        var fullSnapshotDirectory = Path.GetFullPath(snapshotDirectory);
        var manifest = LoadManifest(fullSnapshotDirectory);
        var restoredSpec = manifest.OriginalSpec.NormalizeForPersistence() with
        {
            MapPath = Path.GetFullPath(restoreMapPath)
        };
        restoredSpec.Validate();

        var storage = restoredSpec.Storage.Normalize();
        if (storage.Layout == TableStorageLayout.SingleFile)
            RestoreSingleFileStorage(fullSnapshotDirectory, manifest, restoredSpec, overwrite);
        else if (storage.Layout == TableStorageLayout.Chunked)
            RestoreChunkedStorage(fullSnapshotDirectory, manifest, restoredSpec, overwrite);
        else
            throw new ArgumentOutOfRangeException(nameof(restoreMapPath), storage.Layout, "Unknown MetaDB table storage layout.");

        return restoredSpec;
    }

    private static void CaptureSingleFileStorage(
        TableSpec spec,
        string snapshotDirectory,
        List<MetaDbSnapshotFile> files)
    {
        var sourceMapPath = Path.GetFullPath(spec.MapPath);
        if (!File.Exists(sourceMapPath))
            return;

        var destination = Path.Combine(snapshotDirectory, StorageDirectoryName, SingleFileMapFileName);
        CopyFile(sourceMapPath, destination, overwrite: true);
        files.Add(DescribeSnapshotFile(snapshotDirectory, destination, "single-file-map"));
    }

    private static void CaptureChunkedStorage(
        TableSpec spec,
        string snapshotDirectory,
        List<MetaDbSnapshotFile> files)
    {
        var sourceDirectory = Path.GetFullPath(spec.MapPath);
        if (!Directory.Exists(sourceDirectory))
            return;

        var manifestPath = Path.Combine(sourceDirectory, "manifest.json");
        if (File.Exists(manifestPath))
        {
            var destination = Path.Combine(snapshotDirectory, StorageDirectoryName, "manifest.json");
            CopyFile(manifestPath, destination, overwrite: true);
            files.Add(DescribeSnapshotFile(snapshotDirectory, destination, "chunked-manifest"));
        }

        var chunksDirectory = Path.Combine(sourceDirectory, "chunks");
        if (!Directory.Exists(chunksDirectory))
            return;

        foreach (var sourceFile in Directory.EnumerateFiles(chunksDirectory, "*", SearchOption.AllDirectories))
        {
            var relativeToTable = Path.GetRelativePath(sourceDirectory, sourceFile);
            var destination = Path.Combine(snapshotDirectory, StorageDirectoryName, relativeToTable);
            CopyFile(sourceFile, destination, overwrite: true);
            files.Add(DescribeSnapshotFile(snapshotDirectory, destination, IsChunkFile(sourceFile) ? "chunk" : "chunked-file"));
        }
    }

    private static void RestoreSingleFileStorage(
        string snapshotDirectory,
        MetaDbSnapshotManifest manifest,
        TableSpec restoredSpec,
        bool overwrite)
    {
        var targetMapPath = Path.GetFullPath(restoredSpec.MapPath);
        var targetDirectory = Path.GetDirectoryName(targetMapPath);
        if (!string.IsNullOrWhiteSpace(targetDirectory))
            Directory.CreateDirectory(targetDirectory);

        var mapFile = manifest.Files.FirstOrDefault(static file => file.Role == "single-file-map");
        if (mapFile is not null)
        {
            var source = Path.Combine(snapshotDirectory, FromManifestPath(mapFile.RelativePath));
            CopyFile(source, targetMapPath, overwrite);
        }

        restoredSpec.SaveToFile(targetMapPath + ".tablespec.json", overwrite);
    }

    private static void RestoreChunkedStorage(
        string snapshotDirectory,
        MetaDbSnapshotManifest manifest,
        TableSpec restoredSpec,
        bool overwrite)
    {
        var targetDirectory = Path.GetFullPath(restoredSpec.MapPath);
        Directory.CreateDirectory(targetDirectory);

        if (overwrite)
        {
            var targetChunksDirectory = Path.Combine(targetDirectory, "chunks");
            if (Directory.Exists(targetChunksDirectory))
                Directory.Delete(targetChunksDirectory, recursive: true);
        }

        foreach (var file in manifest.Files)
        {
            if (!file.RelativePath.StartsWith(StorageDirectoryName + "/", StringComparison.OrdinalIgnoreCase))
                continue;

            if (file.Role == "single-file-map")
                continue;

            var relativeUnderStorage = file.RelativePath[(StorageDirectoryName.Length + 1)..];
            var source = Path.Combine(snapshotDirectory, FromManifestPath(file.RelativePath));
            var destination = Path.Combine(targetDirectory, FromManifestPath(relativeUnderStorage));
            CopyFile(source, destination, overwrite);
        }

        restoredSpec.SaveToFile(Path.Combine(targetDirectory, SpecFileName), overwrite);
    }

    private static void PrepareSnapshotDirectory(string snapshotDirectory, bool overwrite)
    {
        if (Directory.Exists(snapshotDirectory))
        {
            if (!overwrite && Directory.EnumerateFileSystemEntries(snapshotDirectory).Any())
                throw new IOException($"Snapshot directory already exists and is not empty: {snapshotDirectory}");

            if (overwrite)
            {
                DeleteFileIfExists(Path.Combine(snapshotDirectory, ManifestFileName));
                DeleteFileIfExists(Path.Combine(snapshotDirectory, SpecFileName));

                var storageDirectory = Path.Combine(snapshotDirectory, StorageDirectoryName);
                if (Directory.Exists(storageDirectory))
                    Directory.Delete(storageDirectory, recursive: true);
            }
        }

        Directory.CreateDirectory(Path.Combine(snapshotDirectory, StorageDirectoryName));
    }

    private static void WriteManifest(string snapshotDirectory, MetaDbSnapshotManifest manifest)
    {
        var path = Path.Combine(snapshotDirectory, ManifestFileName);
        var json = JsonSerializer.Serialize(manifest, Json);
        File.WriteAllText(path, json, Encoding.UTF8);
    }

    private static MetaDbSnapshotFile DescribeSnapshotFile(string snapshotDirectory, string filePath, string role)
    {
        var info = new FileInfo(filePath);
        var relative = ToManifestPath(Path.GetRelativePath(snapshotDirectory, info.FullName));
        return new MetaDbSnapshotFile(relative, role, info.Length);
    }

    private static void CopyFile(string source, string destination, bool overwrite)
    {
        var destinationDirectory = Path.GetDirectoryName(destination);
        if (!string.IsNullOrWhiteSpace(destinationDirectory))
            Directory.CreateDirectory(destinationDirectory);

        File.Copy(source, destination, overwrite);
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static bool IsChunkFile(string path) =>
        string.Equals(Path.GetExtension(path), ".chk", StringComparison.OrdinalIgnoreCase);

    private static string ToManifestPath(string path) =>
        path.Replace(Path.DirectorySeparatorChar, '/').Replace(Path.AltDirectorySeparatorChar, '/');

    private static string FromManifestPath(string path) =>
        path.Replace('/', Path.DirectorySeparatorChar);
}
