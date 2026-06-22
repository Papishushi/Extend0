using System.Text.Json;
using System.Text.Json.Serialization;
using Extend0.Lifecycle.Assurance;
using Extend0.Metadata.Schema;

namespace Extend0.Cli;

public static class MetaDbValidateCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private static readonly JsonSerializerOptions ManifestJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private const uint SingleFileMagic = 0x4C42544D;
    private const ushort SingleFileVersion = 1;
    private const int SingleFileHeaderSize = 16;
    private const int SingleFileColumnDescSize = 20;

    static MetaDbValidateCommand()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public static Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        string workingDirectory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentException.ThrowIfNullOrWhiteSpace(workingDirectory);

        var parse = MetaDbValidateOptions.TryParse(args, workingDirectory, out var options, out var parseError);
        if (!parse)
        {
            error.WriteLine(parseError);
            error.WriteLine();
            WriteHelp(error);
            return Task.FromResult(2);
        }

        if (options.ShowHelp)
        {
            WriteHelp(output);
            return Task.FromResult(0);
        }

        cancellationToken.ThrowIfCancellationRequested();

        if (!MetaDbInspectCommand.TryResolveSpecPath(options.InputPath!, out var specPath, out var resolutionError))
        {
            error.WriteLine(resolutionError);
            return Task.FromResult(1);
        }

        try
        {
            var spec = TableSpec.Helpers.LoadFromFile(specPath);
            var report = BuildReport(options.InputPath!, specPath, spec, options);

            if (options.Json)
                output.WriteLine(JsonSerializer.Serialize(report, JsonOptions));
            else
                WriteHumanReport(output, report);

            return Task.FromResult(report.ErrorCount > 0 ? 1 : 0);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not validate MetaDB spec: {ex.Message}");
            return Task.FromResult(1);
        }
    }

    private static MetaDbValidateReport BuildReport(string inputPath, string specPath, TableSpec spec, MetaDbValidateOptions options)
    {
        var inspect = MetaDbInspectReport.FromSpec(inputPath, specPath, spec);
        var findings = new List<ValidationFinding>
        {
            ValidationFinding.Info("spec-loaded", $"Loaded TableSpec '{inspect.Name}'.")
        };

        ValidateStorage(inspect, findings);
        ValidateColumns(inspect, findings);
        ValidateSidecarConventions(inspect, spec, findings);
        var runtime = ValidateRuntimeStorage(inspect, findings);
        var security = ValidateStorageProtection(inspect, spec, options, findings);
        var continuity = ValidateStorageContinuity(inspect, spec, options, findings);
        var attestation = ValidateHardwareAttestation(inspect, spec, options, findings);

        return MetaDbValidateReport.Create(
            inspect.InputPath,
            inspect.SpecPath,
            inspect.Name,
            inspect.MapPath,
            inspect.Storage,
            inspect.ColumnCount,
            inspect.Columns,
            EstimateLogicalBytes(inspect),
            EstimateStorageBytes(inspect),
            runtime,
            security,
            continuity,
            attestation,
            findings);
    }

    private static StorageProtectionEvidence? ValidateStorageProtection(
        MetaDbInspectReport inspect,
        TableSpec spec,
        MetaDbValidateOptions options,
        List<ValidationFinding> findings)
    {
        var overridePolicy = options.ToOverridePolicy();
        var policy = overridePolicy.RequiresProtection ? overridePolicy : spec.Protection;
        if (!options.Security && !policy.RequiresProtection)
            return null;

        var evidence = StorageProtectionVerifier.DiagnosePath(
            inspect.MapPath,
            policy,
            options.ProtectionManifestPath);

        findings.AddRange(evidence.Findings.Select(ToValidationFinding));
        findings.Add(evidence.Decision == StorageProtectionDecision.FailClosed
            ? ValidationFinding.Error("storage-protection-decision", $"Storage protection decision is {evidence.Decision}.")
            : ValidationFinding.Info("storage-protection-decision", $"Storage protection decision is {evidence.Decision}."));

        return evidence;
    }

    private static HardwareAttestationEvidence? ValidateHardwareAttestation(
        MetaDbInspectReport inspect,
        TableSpec spec,
        MetaDbValidateOptions options,
        List<ValidationFinding> findings)
    {
        var overridePolicy = options.ToAttestationPolicy();
        var policy = overridePolicy.RequiresAttestation ? overridePolicy : spec.Attestation;
        if (!options.Attestation && !policy.RequiresAttestation)
            return null;

        var evidence = HardwareAttestationVerifier.DiagnosePath(
            inspect.MapPath,
            policy,
            options.AttestationManifestPath);

        findings.AddRange(evidence.Findings.Select(ToValidationFinding));
        findings.Add(evidence.Decision == HardwareAttestationDecision.FailClosed
            ? ValidationFinding.Error("hardware-attestation-decision", $"Hardware attestation decision is {evidence.Decision}.")
            : ValidationFinding.Info("hardware-attestation-decision", $"Hardware attestation decision is {evidence.Decision}."));

        return evidence;
    }

    private static StorageContinuityEvidence? ValidateStorageContinuity(
        MetaDbInspectReport inspect,
        TableSpec spec,
        MetaDbValidateOptions options,
        List<ValidationFinding> findings)
    {
        var overridePolicy = options.ToContinuityPolicy();
        var policy = overridePolicy.RequiresContinuity ? overridePolicy : spec.Continuity;
        if (!options.OwnershipTransfer && !policy.RequiresContinuity)
            return null;

        if (options.OwnershipTransfer && !policy.RequiresContinuity)
        {
            findings.Add(ValidationFinding.Info(
                "ownership-transfer-ephemeral",
                "Ownership transfer diagnostics are running without requiring durable state continuity; this is valid for ephemeral or externally reconstructed services."));
        }

        var evidence = StorageContinuityVerifier.DiagnosePath(
            inspect.MapPath,
            policy,
            options.ContinuityManifestPath);

        findings.AddRange(evidence.Findings.Select(ToValidationFinding));
        findings.Add(evidence.Decision == StorageContinuityDecision.FailClosed
            ? ValidationFinding.Error("storage-continuity-decision", $"Storage continuity decision is {evidence.Decision}.")
            : ValidationFinding.Info("storage-continuity-decision", $"Storage continuity decision is {evidence.Decision}."));

        return evidence;
    }

    private static ValidationFinding ToValidationFinding(StorageProtectionFinding finding) =>
        finding.Severity switch
        {
            StorageProtectionFindingSeverity.Info => ValidationFinding.Info(finding.Id, finding.Message),
            StorageProtectionFindingSeverity.Warning => ValidationFinding.Warning(finding.Id, finding.Message),
            StorageProtectionFindingSeverity.Error => ValidationFinding.Error(finding.Id, finding.Message),
            _ => ValidationFinding.Warning(finding.Id, finding.Message)
        };

    private static ValidationFinding ToValidationFinding(HardwareAttestationFinding finding) =>
        finding.Severity switch
        {
            HardwareAttestationFindingSeverity.Info => ValidationFinding.Info(finding.Id, finding.Message),
            HardwareAttestationFindingSeverity.Warning => ValidationFinding.Warning(finding.Id, finding.Message),
            HardwareAttestationFindingSeverity.Error => ValidationFinding.Error(finding.Id, finding.Message),
            _ => ValidationFinding.Warning(finding.Id, finding.Message)
        };

    private static ValidationFinding ToValidationFinding(StorageContinuityFinding finding) =>
        finding.Severity switch
        {
            StorageContinuityFindingSeverity.Info => ValidationFinding.Info(finding.Id, finding.Message),
            StorageContinuityFindingSeverity.Warning => ValidationFinding.Warning(finding.Id, finding.Message),
            StorageContinuityFindingSeverity.Error => ValidationFinding.Error(finding.Id, finding.Message),
            _ => ValidationFinding.Warning(finding.Id, finding.Message)
        };

    private static void ValidateStorage(MetaDbInspectReport inspect, List<ValidationFinding> findings)
    {
        switch (inspect.Storage.Layout)
        {
            case TableStorageLayout.SingleFile:
                findings.Add(ValidationFinding.Info("storage-layout", "Single-file storage layout."));
                if (inspect.Storage.ChunkSize > 0)
                    findings.Add(ValidationFinding.Info("single-file-chunk-alignment", $"Single-file growth is chunk-aligned to {inspect.Storage.ChunkSize} bytes."));
                break;

            case TableStorageLayout.Chunked:
                findings.Add(ValidationFinding.Info("storage-layout", "Chunked storage layout."));
                if (inspect.Storage.ChunkSize <= 0)
                    findings.Add(ValidationFinding.Error("chunk-size", "Chunked storage requires a positive chunk size."));
                break;

            default:
                findings.Add(ValidationFinding.Error("storage-layout", $"Unknown storage layout '{inspect.Storage.Layout}'."));
                break;
        }
    }

    private static void ValidateColumns(MetaDbInspectReport inspect, List<ValidationFinding> findings)
    {
        var duplicateNames = inspect.Columns
            .GroupBy(static column => column.Name, StringComparer.Ordinal)
            .Where(static group => group.Count() > 1)
            .Select(static group => group.Key)
            .ToArray();

        foreach (var duplicate in duplicateNames)
            findings.Add(ValidationFinding.Error("duplicate-column-name", $"Column name '{duplicate}' appears more than once."));

        foreach (var column in inspect.Columns)
        {
            if (string.IsNullOrWhiteSpace(column.Name))
                findings.Add(ValidationFinding.Error("column-name", $"Column {column.Index} has an empty name."));

            if (column.KeyBytes < 0)
                findings.Add(ValidationFinding.Error("column-key-size", $"Column '{column.Name}' has a negative key size."));
            else if (column.KeyBytes == 0)
                findings.Add(ValidationFinding.Info("value-only-column", $"Column '{column.Name}' is value-only with no key bytes."));

            if (column.ValueBytes <= 0)
                findings.Add(ValidationFinding.Error("column-value-size", $"Column '{column.Name}' must reserve at least one value byte."));

            if (column.EntryBytes <= 0)
                findings.Add(ValidationFinding.Error("column-entry-size", $"Column '{column.Name}' has a non-positive entry size."));

            if (column.InitialCapacity == 0)
                findings.Add(ValidationFinding.Warning("column-capacity", $"Column '{column.Name}' has zero initial capacity."));

            if (inspect.Storage.Layout == TableStorageLayout.Chunked && inspect.Storage.ChunkSize > 0)
            {
                if (column.EntryBytes > inspect.Storage.ChunkSize)
                {
                    findings.Add(ValidationFinding.Error(
                        "chunk-entry-size",
                        $"Column '{column.Name}' entry size {column.EntryBytes} bytes is larger than chunk size {inspect.Storage.ChunkSize} bytes."));
                }
                else if (inspect.Storage.ChunkSize % column.EntryBytes != 0)
                {
                    findings.Add(ValidationFinding.Warning(
                        "chunk-entry-fit",
                        $"Column '{column.Name}' entry size {column.EntryBytes} bytes does not divide chunk size {inspect.Storage.ChunkSize}; trailing bytes will be unused."));
                }
            }

            if (inspect.Storage.Layout == TableStorageLayout.SingleFile
                && inspect.Storage.ChunkSize > 0
                && column.EntryBytes > inspect.Storage.ChunkSize)
            {
                findings.Add(ValidationFinding.Warning(
                    "single-file-entry-chunk-fit",
                    $"Column '{column.Name}' entry size {column.EntryBytes} bytes is larger than the single-file chunk alignment {inspect.Storage.ChunkSize} bytes."));
            }
        }
    }

    private static void ValidateSidecarConventions(MetaDbInspectReport inspect, TableSpec spec, List<ValidationFinding> findings)
    {
        var specPath = Path.GetFullPath(inspect.SpecPath);
        var specDirectory = Path.GetDirectoryName(specPath);

        if (inspect.Storage.Layout == TableStorageLayout.Chunked)
        {
            var runtimeExpected = Path.Combine(Path.GetFullPath(inspect.MapPath), TableSpec.Helpers.ChunkedSpecFileName);
            if (string.Equals(specPath, runtimeExpected, StringComparison.OrdinalIgnoreCase))
            {
                findings.Add(ValidationFinding.Info("sidecar-convention", "Chunked TableSpec uses the runtime canonical tablespec.json inside the table directory."));
                return;
            }

            var saveDirectory = string.IsNullOrWhiteSpace(specDirectory)
                ? null
                : Directory.GetParent(specDirectory)?.FullName;
            if (saveDirectory is not null
                && TableSpec.Helpers.TryInferSaveToDirectoryExtension(spec, saveDirectory, specPath, out var chunkedExtension))
            {
                var message = string.Equals(chunkedExtension, TableSpec.Helpers.DefaultSpecExtension, StringComparison.OrdinalIgnoreCase)
                    ? "Chunked TableSpec uses the SaveToDirectory default path."
                    : $"Chunked TableSpec uses custom SaveToDirectory extension '{chunkedExtension}'.";
                findings.Add(ValidationFinding.Info("sidecar-convention", message));
                return;
            }

            findings.Add(ValidationFinding.Info("sidecar-convention", "Chunked TableSpec path was resolved explicitly or with a custom convention; no fixed extension is enforced."));
            return;
        }

        if (!string.IsNullOrWhiteSpace(specDirectory)
            && TableSpec.Helpers.TryInferSaveToDirectoryExtension(spec, specDirectory, specPath, out var extension))
        {
            var message = string.Equals(extension, TableSpec.Helpers.DefaultSpecExtension, StringComparison.OrdinalIgnoreCase)
                ? "Single-file TableSpec uses the SaveToDirectory default path."
                : $"Single-file TableSpec uses custom SaveToDirectory extension '{extension}'.";
            findings.Add(ValidationFinding.Info("sidecar-convention", message));
            return;
        }

        var mapSidecar = Path.GetFullPath(inspect.MapPath) + ".tablespec.json";
        if (string.Equals(specPath, mapSidecar, StringComparison.OrdinalIgnoreCase))
            findings.Add(ValidationFinding.Info("sidecar-convention", "Single-file TableSpec uses the map-path sidecar."));
        else
            findings.Add(ValidationFinding.Info("sidecar-convention", "Single-file TableSpec path was resolved explicitly or with a custom convention; no fixed extension is enforced."));
    }

    private static long EstimateLogicalBytes(MetaDbInspectReport inspect) =>
        inspect.Columns.Sum(static column => checked((long)column.EntryBytes * column.InitialCapacity));

    private static long EstimateStorageBytes(MetaDbInspectReport inspect)
    {
        var logicalBytes = EstimateLogicalBytes(inspect);
        if (inspect.Storage.Layout == TableStorageLayout.SingleFile)
        {
            return inspect.Storage.ChunkSize > 0
                ? RoundUp(logicalBytes, inspect.Storage.ChunkSize)
                : logicalBytes;
        }

        if (inspect.Storage.ChunkSize <= 0)
            return logicalBytes;

        return inspect.Columns.Sum(column =>
        {
            var columnBytes = checked((long)column.EntryBytes * column.InitialCapacity);
            return RoundUp(columnBytes, inspect.Storage.ChunkSize);
        });
    }

    private static long RoundUp(long value, int multiple)
    {
        if (value == 0 || multiple <= 0)
            return value;

        var remainder = value % multiple;
        return remainder == 0 ? value : checked(value + multiple - remainder);
    }

    private static MetaDbRuntimeStorageReport ValidateRuntimeStorage(MetaDbInspectReport inspect, List<ValidationFinding> findings) =>
        inspect.Storage.Layout switch
        {
            TableStorageLayout.SingleFile => ValidateSingleFileRuntimeStorage(inspect, findings),
            TableStorageLayout.Chunked => ValidateChunkedRuntimeStorage(inspect, findings),
            _ => new MetaDbRuntimeStorageReport(false, inspect.Storage.Layout.ToString(), null, null, null, null, null)
        };

    private static MetaDbRuntimeStorageReport ValidateSingleFileRuntimeStorage(MetaDbInspectReport inspect, List<ValidationFinding> findings)
    {
        var mapPath = Path.GetFullPath(inspect.MapPath);
        if (!File.Exists(mapPath))
        {
            findings.Add(ValidationFinding.Info("single-file-runtime-storage", "Backing map file is not materialized yet; only the TableSpec was validated."));
            return new MetaDbRuntimeStorageReport(false, "SingleFile", mapPath, null, null, null, null);
        }

        var fileInfo = new FileInfo(mapPath);
        findings.Add(ValidationFinding.Info("single-file-runtime-storage", $"Backing map file exists ({fileInfo.Length} bytes)."));

        if (fileInfo.Length < SingleFileHeaderSize)
        {
            findings.Add(ValidationFinding.Error("single-file-header", $"Backing map file is too small to contain a metadata header ({fileInfo.Length} bytes)."));
            return new MetaDbRuntimeStorageReport(true, "SingleFile", mapPath, fileInfo.Length, null, null, null);
        }

        try
        {
            using var stream = new FileStream(mapPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var reader = new BinaryReader(stream);

            var header = ReadSingleFileHeader(reader);
            if (header.Magic == SingleFileMagic)
                findings.Add(ValidationFinding.Info("single-file-magic", "Single-file header magic matches MTBL."));
            else
                findings.Add(ValidationFinding.Error("single-file-magic", $"Single-file header magic is 0x{header.Magic:X8}, expected 0x{SingleFileMagic:X8}."));

            if (header.Version == SingleFileVersion)
                findings.Add(ValidationFinding.Info("single-file-version", $"Single-file format version is {SingleFileVersion}."));
            else
                findings.Add(ValidationFinding.Error("single-file-version", $"Single-file format version is {header.Version}, expected {SingleFileVersion}."));

            if (header.ColumnCount == inspect.ColumnCount)
                findings.Add(ValidationFinding.Info("single-file-column-count", $"Single-file header declares {header.ColumnCount} columns."));
            else
                findings.Add(ValidationFinding.Error("single-file-column-count", $"Single-file header declares {header.ColumnCount} columns, but TableSpec declares {inspect.ColumnCount}."));

            var descriptorTableBytes = checked((long)header.ColumnCount * SingleFileColumnDescSize);
            if (header.ColumnsTableOffset < SingleFileHeaderSize || header.ColumnsTableOffset + descriptorTableBytes > stream.Length)
            {
                findings.Add(ValidationFinding.Error("single-file-column-table", "Single-file column descriptor table is outside the backing file."));
                return new MetaDbRuntimeStorageReport(true, "SingleFile", mapPath, fileInfo.Length, header.ColumnCount, null, null);
            }

            stream.Position = header.ColumnsTableOffset;
            var descriptors = new List<SingleFileColumnDescriptor>(header.ColumnCount);
            for (var i = 0; i < header.ColumnCount; i++)
                descriptors.Add(ReadSingleFileColumnDescriptor(reader));

            var declaredRows = 0L;
            var requiredBytes = header.ColumnsTableOffset + descriptorTableBytes;
            for (var i = 0; i < descriptors.Count; i++)
            {
                var descriptor = descriptors[i];
                declaredRows += descriptor.RowCapacity;
                requiredBytes = Math.Max(requiredBytes, checked(descriptor.BaseOffset + descriptor.EntrySizeBytes * descriptor.RowCapacity));

                if (i >= inspect.Columns.Count)
                    continue;

                var column = inspect.Columns[i];
                if (descriptor.KeySize != column.KeyBytes || descriptor.ValueSize != column.ValueBytes)
                {
                    findings.Add(ValidationFinding.Error(
                        "single-file-column-shape",
                        $"Column {i} runtime shape ({descriptor.KeySize},{descriptor.ValueSize}) does not match TableSpec ({column.KeyBytes},{column.ValueBytes})."));
                }

                if (descriptor.RowCapacity < column.InitialCapacity)
                {
                    findings.Add(ValidationFinding.Error(
                        "single-file-column-capacity",
                        $"Column '{column.Name}' runtime capacity {descriptor.RowCapacity} is below TableSpec initial capacity {column.InitialCapacity}."));
                }

                if (descriptor.EntrySizeBytes <= 0)
                    findings.Add(ValidationFinding.Error("single-file-column-entry-size", $"Column {i} has non-positive runtime entry size."));

                if (descriptor.BaseOffset < SingleFileHeaderSize)
                    findings.Add(ValidationFinding.Error("single-file-column-offset", $"Column {i} base offset {descriptor.BaseOffset} is invalid."));
            }

            if (requiredBytes <= stream.Length)
                findings.Add(ValidationFinding.Info("single-file-physical-size", $"Backing map file covers the declared runtime capacity ({requiredBytes} required bytes)."));
            else
                findings.Add(ValidationFinding.Error("single-file-physical-size", $"Backing map file is {stream.Length} bytes but declared runtime capacity requires {requiredBytes} bytes."));

            return new MetaDbRuntimeStorageReport(true, "SingleFile", mapPath, fileInfo.Length, header.ColumnCount, declaredRows, requiredBytes);
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Error("single-file-runtime-storage", $"Could not read backing map file: {ex.Message}"));
            return new MetaDbRuntimeStorageReport(true, "SingleFile", mapPath, fileInfo.Length, null, null, null);
        }
    }

    private static MetaDbRuntimeStorageReport ValidateChunkedRuntimeStorage(MetaDbInspectReport inspect, List<ValidationFinding> findings)
    {
        var tableDirectory = Path.GetFullPath(inspect.MapPath);
        var manifestPath = Path.Combine(tableDirectory, "manifest.json");
        var chunksDirectory = Path.Combine(tableDirectory, "chunks");

        if (!File.Exists(manifestPath))
        {
            if (Directory.Exists(chunksDirectory) && Directory.EnumerateFiles(chunksDirectory, "*.chk").Any())
                findings.Add(ValidationFinding.Error("chunked-manifest", "Chunk files exist but manifest.json is missing."));
            else
                findings.Add(ValidationFinding.Info("chunked-runtime-storage", "Chunked table is not materialized yet; only the TableSpec was validated."));

            return new MetaDbRuntimeStorageReport(false, "Chunked", tableDirectory, null, null, null, null);
        }

        ChunkedManifest? manifest;
        try
        {
            manifest = JsonSerializer.Deserialize<ChunkedManifest>(File.ReadAllText(manifestPath), ManifestJsonOptions);
        }
        catch (Exception ex)
        {
            findings.Add(ValidationFinding.Error("chunked-manifest", $"Could not read manifest.json: {ex.Message}"));
            return new MetaDbRuntimeStorageReport(true, "Chunked", tableDirectory, null, null, null, null);
        }

        if (manifest is null)
        {
            findings.Add(ValidationFinding.Error("chunked-manifest", "manifest.json is empty or invalid."));
            return new MetaDbRuntimeStorageReport(true, "Chunked", tableDirectory, null, null, null, null);
        }

        findings.Add(ValidationFinding.Info("chunked-manifest", $"Found chunked manifest '{manifestPath}'."));

        if (manifest.Version == 1)
            findings.Add(ValidationFinding.Info("chunked-manifest-version", "Chunked manifest version is 1."));
        else
            findings.Add(ValidationFinding.Error("chunked-manifest-version", $"Chunked manifest version is {manifest.Version}, expected 1."));

        if (manifest.ChunkSize == inspect.Storage.ChunkSize)
            findings.Add(ValidationFinding.Info("chunked-manifest-chunk-size", $"Manifest chunk size is {manifest.ChunkSize}."));
        else
            findings.Add(ValidationFinding.Error("chunked-manifest-chunk-size", $"Manifest chunk size is {manifest.ChunkSize}, but TableSpec declares {inspect.Storage.ChunkSize}."));

        if (manifest.Columns.Length == inspect.ColumnCount)
            findings.Add(ValidationFinding.Info("chunked-manifest-column-count", $"Manifest declares {manifest.Columns.Length} columns."));
        else
            findings.Add(ValidationFinding.Error("chunked-manifest-column-count", $"Manifest declares {manifest.Columns.Length} columns, but TableSpec declares {inspect.ColumnCount}."));

        if (!Directory.Exists(chunksDirectory))
        {
            findings.Add(ValidationFinding.Error("chunked-chunks-directory", $"Missing chunks directory '{chunksDirectory}'."));
            return new MetaDbRuntimeStorageReport(true, "Chunked", tableDirectory, 0, manifest.Columns.Length, manifest.Columns.Sum(static c => (long)c.RowCapacity), 0);
        }

        var expectedChunks = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var physicalBytes = 0L;
        var requiredBytes = 0L;
        var declaredRows = 0L;

        for (var i = 0; i < manifest.Columns.Length; i++)
        {
            var column = manifest.Columns[i];
            declaredRows += column.RowCapacity;

            if (i < inspect.Columns.Count)
            {
                var specColumn = inspect.Columns[i];
                if (column.KeySize != specColumn.KeyBytes || column.ValueSize != specColumn.ValueBytes)
                {
                    findings.Add(ValidationFinding.Error(
                        "chunked-column-shape",
                        $"Column {i} manifest shape ({column.KeySize},{column.ValueSize}) does not match TableSpec ({specColumn.KeyBytes},{specColumn.ValueBytes})."));
                }

                if (column.RowCapacity < specColumn.InitialCapacity)
                {
                    findings.Add(ValidationFinding.Error(
                        "chunked-column-capacity",
                        $"Column '{specColumn.Name}' manifest capacity {column.RowCapacity} is below TableSpec initial capacity {specColumn.InitialCapacity}."));
                }
            }

            var entrySize = checked(column.KeySize + column.ValueSize);
            if (entrySize <= 0)
            {
                findings.Add(ValidationFinding.Error("chunked-column-entry-size", $"Column {i} has non-positive manifest entry size."));
                continue;
            }

            if (manifest.ChunkSize < entrySize)
            {
                findings.Add(ValidationFinding.Error("chunked-column-entry-size", $"Column {i} entry size {entrySize} is larger than manifest chunk size {manifest.ChunkSize}."));
                continue;
            }

            var rowsPerChunk = (uint)(manifest.ChunkSize / entrySize);
            var requiredChunkCount = column.RowCapacity == 0
                ? 0
                : checked((int)((column.RowCapacity + rowsPerChunk - 1) / rowsPerChunk));
            requiredBytes += checked((long)requiredChunkCount * manifest.ChunkSize);

            for (var chunk = 0; chunk < requiredChunkCount; chunk++)
            {
                var chunkPath = Path.Combine(chunksDirectory, $"c{i:D4}_{chunk:D6}.chk");
                expectedChunks.Add(Path.GetFullPath(chunkPath));

                if (!File.Exists(chunkPath))
                {
                    findings.Add(ValidationFinding.Error("chunked-chunk-missing", $"Missing chunk file '{chunkPath}'."));
                    continue;
                }

                var length = new FileInfo(chunkPath).Length;
                physicalBytes += length;
                if (length != manifest.ChunkSize)
                    findings.Add(ValidationFinding.Error("chunked-chunk-size", $"Chunk '{chunkPath}' is {length} bytes, expected {manifest.ChunkSize}."));
            }
        }

        foreach (var chunkPath in Directory.EnumerateFiles(chunksDirectory, "*.chk"))
        {
            var fullChunkPath = Path.GetFullPath(chunkPath);
            if (!expectedChunks.Contains(fullChunkPath))
            {
                physicalBytes += new FileInfo(fullChunkPath).Length;
                findings.Add(ValidationFinding.Warning("chunked-orphan-chunk", $"Chunk file '{fullChunkPath}' is not referenced by the manifest capacity."));
            }
        }

        findings.Add(ValidationFinding.Info("chunked-physical-size", $"Chunked runtime storage has {physicalBytes} physical bytes; manifest capacity requires {requiredBytes} bytes."));
        return new MetaDbRuntimeStorageReport(true, "Chunked", tableDirectory, physicalBytes, manifest.Columns.Length, declaredRows, requiredBytes);
    }

    private static SingleFileHeader ReadSingleFileHeader(BinaryReader reader) =>
        new(
            reader.ReadUInt32(),
            reader.ReadUInt16(),
            reader.ReadUInt16(),
            reader.ReadInt64());

    private static SingleFileColumnDescriptor ReadSingleFileColumnDescriptor(BinaryReader reader) =>
        new(
            reader.ReadInt32(),
            reader.ReadInt32(),
            reader.ReadUInt32(),
            reader.ReadInt64());

    private static void WriteHumanReport(TextWriter output, MetaDbValidateReport report)
    {
        output.WriteLine("Extend0 MetaDB validate");
        output.WriteLine($"Input: {report.InputPath}");
        output.WriteLine($"Spec: {report.SpecPath}");
        output.WriteLine($"Name: {report.Name}");
        output.WriteLine($"MapPath: {report.MapPath}");
        output.WriteLine($"Storage: {report.Storage.Layout}");
        output.WriteLine($"ChunkSize: {report.Storage.ChunkSize}");
        output.WriteLine($"Columns: {report.ColumnCount}");
        output.WriteLine($"Estimated logical bytes: {report.EstimatedLogicalBytes}");
        output.WriteLine($"Estimated storage bytes: {report.EstimatedStorageBytes}");
        output.WriteLine($"Runtime storage exists: {report.RuntimeStorage.Exists}");
        if (report.RuntimeStorage.PhysicalBytes is not null)
            output.WriteLine($"Runtime physical bytes: {report.RuntimeStorage.PhysicalBytes}");
        if (report.StorageProtection is not null)
        {
            output.WriteLine($"Required protection: {report.StorageProtection.Policy.RequiredLevel}");
            output.WriteLine($"Observed protection: {report.StorageProtection.ObservedLevel}");
            output.WriteLine($"Protection decision: {report.StorageProtection.Decision}");
        }
        if (report.StorageContinuity is not null)
        {
            output.WriteLine($"Required continuity: {report.StorageContinuity.Policy.RequiredLevel}");
            output.WriteLine($"Observed continuity: {report.StorageContinuity.ObservedLevel}");
            output.WriteLine($"Continuity decision: {report.StorageContinuity.Decision}");
        }
        if (report.HardwareAttestation is not null)
        {
            output.WriteLine($"Required attestation: {report.HardwareAttestation.Policy.RequiredLevel}");
            output.WriteLine($"Required attestation technology: {report.HardwareAttestation.Policy.RequiredTechnology}");
            output.WriteLine($"Observed attestation: {report.HardwareAttestation.ObservedLevel}");
            output.WriteLine($"Observed attestation technology: {report.HardwareAttestation.ObservedTechnology}");
            output.WriteLine($"Attestation decision: {report.HardwareAttestation.Decision}");
        }
        output.WriteLine();

        foreach (var finding in report.Findings)
            output.WriteLine($"[{FormatSeverity(finding.Severity)}] {finding.Id}: {finding.Message}");

        output.WriteLine();
        output.WriteLine($"Summary: {report.InfoCount} info, {report.WarningCount} warnings, {report.ErrorCount} errors");
    }

    private static string FormatSeverity(ValidationSeverity severity) =>
        severity switch
        {
            ValidationSeverity.Info => "info",
            ValidationSeverity.Warning => "warn",
            ValidationSeverity.Error => "error",
            _ => severity.ToString().ToLowerInvariant()
        };

    private static void WriteHelp(TextWriter writer)
    {
        writer.WriteLine("Usage:");
        writer.WriteLine("  extend0 metadb validate <path> [--security] [--ownership-transfer] [--json]");
        writer.WriteLine();
        writer.WriteLine("Arguments:");
        writer.WriteLine("  <path>    TableSpec file, map path resolved via TableSpec save conventions, or chunked table directory.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  --security                         Run storage protection diagnostics even when the TableSpec does not require protection.");
        writer.WriteLine("  --require-protection <level>       Override required level: none, declared, provider-attested, platform-verified, managed.");
        writer.WriteLine("  --provider <id>                    Required provider id. Requires --require-protection.");
        writer.WriteLine("  --protection-id <id>               Required protected volume/mount id. Requires --require-protection.");
        writer.WriteLine("  --protection-manifest <path>       Explicit storage protection manifest.");
        writer.WriteLine("  --ownership-transfer               Run owner-movement diagnostics without requiring durable state continuity.");
        writer.WriteLine("  --state-continuity                 Require shared or replicated storage suitable for durable state transfer.");
        writer.WriteLine("  --require-continuity <level>       Override required continuity: none, local-only, restorable-snapshot, shared-backing-store, symmetric-replication.");
        writer.WriteLine("  --continuity-provider <id>         Required continuity provider id.");
        writer.WriteLine("  --continuity-id <id>               Required shared-store or replication-group id.");
        writer.WriteLine("  --continuity-manifest <path>       Explicit storage continuity manifest.");
        writer.WriteLine("  --attestation                      Run hardware-attestation diagnostics.");
        writer.WriteLine("  --require-attestation <level>      Override required attestation: none, declared, provider-attested, platform-verified, remote-attested.");
        writer.WriteLine("  --attestation-technology <kind>    Required technology: intel-sgx, intel-tdx, amd-sev-snp, arm-trustzone, arm-cca, tpm-sealed, custom.");
        writer.WriteLine("  --attestation-provider <id>        Required attestation provider id.");
        writer.WriteLine("  --attestation-id <id>              Required attestation identity.");
        writer.WriteLine("  --measurement <value>              Required code/platform measurement.");
        writer.WriteLine("  --attestation-policy-id <id>       Required provider-defined attestation policy id.");
        writer.WriteLine("  --attestation-manifest <path>      Explicit hardware attestation manifest.");
        writer.WriteLine("  --json                             Emit a machine-readable JSON report.");
        writer.WriteLine("  -h, --help");
    }
}

public sealed record MetaDbValidateReport(
    string InputPath,
    string SpecPath,
    string Name,
    string MapPath,
    TableStorageOptions Storage,
    int ColumnCount,
    IReadOnlyList<MetaDbColumnReport> Columns,
    long EstimatedLogicalBytes,
    long EstimatedStorageBytes,
    MetaDbRuntimeStorageReport RuntimeStorage,
    StorageProtectionEvidence? StorageProtection,
    StorageContinuityEvidence? StorageContinuity,
    HardwareAttestationEvidence? HardwareAttestation,
    IReadOnlyList<ValidationFinding> Findings,
    int InfoCount,
    int WarningCount,
    int ErrorCount)
{
    public static MetaDbValidateReport Create(
        string inputPath,
        string specPath,
        string name,
        string mapPath,
        TableStorageOptions storage,
        int columnCount,
        IReadOnlyList<MetaDbColumnReport> columns,
        long estimatedLogicalBytes,
        long estimatedStorageBytes,
        MetaDbRuntimeStorageReport runtimeStorage,
        StorageProtectionEvidence? storageProtection,
        StorageContinuityEvidence? storageContinuity,
        HardwareAttestationEvidence? hardwareAttestation,
        IReadOnlyList<ValidationFinding> findings) =>
        new(
            inputPath,
            specPath,
            name,
            mapPath,
            storage,
            columnCount,
            columns,
            estimatedLogicalBytes,
            estimatedStorageBytes,
            runtimeStorage,
            storageProtection,
            storageContinuity,
            hardwareAttestation,
            findings,
            findings.Count(static f => f.Severity == ValidationSeverity.Info),
            findings.Count(static f => f.Severity == ValidationSeverity.Warning),
            findings.Count(static f => f.Severity == ValidationSeverity.Error));
}

public sealed record MetaDbRuntimeStorageReport(
    bool Exists,
    string Layout,
    string? Path,
    long? PhysicalBytes,
    int? RuntimeColumnCount,
    long? RuntimeDeclaredRows,
    long? RequiredBytes);

internal sealed record SingleFileHeader(uint Magic, ushort Version, ushort ColumnCount, long ColumnsTableOffset);

internal sealed record SingleFileColumnDescriptor(int KeySize, int ValueSize, uint RowCapacity, long BaseOffset)
{
    public long EntrySizeBytes => checked((long)KeySize + ValueSize);
}

internal sealed record ChunkedManifest
{
    public int Version { get; init; }

    public int ChunkSize { get; init; }

    public ChunkedManifestColumn[] Columns { get; init; } = [];
}

internal sealed record ChunkedManifestColumn
{
    public int KeySize { get; init; }

    public int ValueSize { get; init; }

    public uint RowCapacity { get; init; }
}
