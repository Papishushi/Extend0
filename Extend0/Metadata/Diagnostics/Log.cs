using Microsoft.Extensions.Logging;

namespace Extend0.Metadata.Diagnostics
{
    /// <summary>
    /// Precompiled logging helpers for <see cref="MetaDBManager"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Events are grouped by id range:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><c>2000–2099</c>: table registry and open/attach operations.</description></item>
    ///   <item><description><c>2200–2299</c>: column operations such as fill and copy.</description></item>
    ///   <item><description><c>2300–2399</c>: parent→child reference vector operations.</description></item>
    ///   <item><description><c>2400–2499</c>: capacity/auto-grow diagnostics.</description></item>
    ///   <item><description><c>2500–2599</c>: generic named operations (<c>Run</c>/<c>RunAsync</c>).</description></item>
    /// </list>
    /// Using <see cref="LoggerMessageAttribute"/> keeps logging overhead low on hot paths.
    /// </remarks>
    public static partial class Log
    {
        // ───────── Registry / Open (2000-2099) ─────────

        [LoggerMessage(2000, LogLevel.Warning,
            "MetaDbManager.GrowColumnTo for this instance was already configured. It will be overwritten.")]
        public static partial void GrowHookOverwrite(ILogger logger);

        [LoggerMessage(2001, LogLevel.Information,
            "'{Name}' was lazily registered. Id={Id} Path={Path}")]
        public static partial void TableRegisteredLazy(ILogger logger, string name, Guid id, string path);

        [LoggerMessage(2002, LogLevel.Information,
            "'{Name}' created immediately. Id={Id} Path={Path}")]
        public static partial void TableCreatedNow(ILogger logger, string name, Guid id, string path);

        [LoggerMessage(2003, LogLevel.Debug,
            "Trying to register table '{Name}'.")]
        public static partial void TableRegistering(ILogger logger, string name);

        [LoggerMessage(2004, LogLevel.Warning,
            "Duplicate table name '{Name}'.")]
        public static partial void TableNameDuplicate(ILogger logger, string name);

        [LoggerMessage(2100, LogLevel.Information,
            "'{Name}' was opened from {Path}. Id={Id}")]
        public static partial void TableOpened(ILogger logger, string name, string path, Guid id);


        // ───────── Column ops: Fill / Copy (2200-2299) ─────────

        [LoggerMessage(2200, LogLevel.Debug,
            "FillColumn<{Type}>: {Table} col={Column} rows={Rows} policy={Policy}")]
        public static partial void FillColumnStart(ILogger logger, string table, uint column, uint rows, string type, CapacityPolicy policy);

        [LoggerMessage(2201, LogLevel.Debug,
            "FillColumn<{Type}>: {Table} OK in {Ms} ms")]
        public static partial void FillColumnEnd(ILogger logger, string table, string type, double ms);

        [LoggerMessage(2202, LogLevel.Error,
            "FillColumn<{Type}>: {Table} VALUE {ValueSize} < sizeof({TypeSize})")]
        public static partial void FillColumnValueTooSmall(ILogger logger, string table, string type, int valueSize, int typeSize);

        [LoggerMessage(2210, LogLevel.Debug,
            "FillColumn(raw): {Table} col={Column} rows={Rows} policy={Policy}")]
        public static partial void FillRawStart(ILogger logger, string table, uint column, uint rows, CapacityPolicy policy);

        [LoggerMessage(2211, LogLevel.Debug,
            "FillColumn(raw): {Table} OK in {Ms} ms")]
        public static partial void FillRawEnd(ILogger logger, string table, double ms);

        [LoggerMessage(2220, LogLevel.Debug,
            "CopyColumn: {SrcName}[{SrcCol}] -> {DstName}[{DstCol}] rows={Rows} policy={Policy}")]
        public static partial void CopyStart(ILogger logger, string srcName, uint srcCol, string dstName, uint dstCol, uint rows, CapacityPolicy policy);

        [LoggerMessage(2221, LogLevel.Debug,
            "CopyColumn: {SrcName}->{DstName} OK in {Ms} ms")]
        public static partial void CopyEnd(ILogger logger, string srcName, string dstName, double ms);

        [LoggerMessage(2222, LogLevel.Error,
            "CopyColumn: row {Row} VALUE sizes differ (src {SrcSize} != dst {DstSize})")]
        public static partial void CopySizeMismatch(ILogger logger, uint row, int srcSize, int dstSize);


        // ───────── Refs (2300-2399) ─────────

        [LoggerMessage(2300, LogLevel.Debug,
            "EnsureRefVec: {Table} col={RefsCol} row={Row} init vec")]
        public static partial void EnsureRefVecInit(ILogger logger, string table, uint refsCol, uint row);

        [LoggerMessage(2301, LogLevel.Error,
            "REFS: ValueSize is too small to host refs (len={Len})")]
        public static partial void RefCellTooSmall(ILogger logger, int len);

        [LoggerMessage(2302, LogLevel.Debug,
            "LinkRef: {Parent} row={Row} -> ChildId={ChildId} (col={Col} row={ChildRow})")]
        public static partial void LinkRefAdded(ILogger logger, string parent, uint row, Guid childId, uint col, uint childRow);

        [LoggerMessage(2303, LogLevel.Error,
            "Refs vector is full; increment ValueBytes or implement overflow.")]
        public static partial void RefsFull(ILogger logger);

        [LoggerMessage(2310, LogLevel.Information,
            "Reusing children for {Parent} row={Row}: ChildId={ChildId}")]
        public static partial void ChildReused(ILogger logger, string parent, uint row, Guid childId);

        [LoggerMessage(2311, LogLevel.Information,
            "Child created and linked: Parent={Parent} Row={Row} -> Child={Child} ({ChildId})")]
        public static partial void ChildCreatedLinked(ILogger logger, string parent, uint row, string child, Guid childId);


        // ───────── Capacity/Grow (2400-2499) ─────────

        [LoggerMessage(2400, LogLevel.Debug,
            "EnsureRowCapacity({Table}) col={Col} need={Rows} ⇒ OK (without grow)")]
        public static partial void CapacityOk(ILogger logger, string table, uint col, uint rows);

        [LoggerMessage(2401, LogLevel.Information,
            "EnsureRowCapacity({Table}) col={Col} growing >= {Rows} (policy={Policy})")]
        public static partial void CapacityGrow(ILogger logger, string table, uint col, uint rows, CapacityPolicy policy);

        [LoggerMessage(2402, LogLevel.Error,
            "CapacityPolicy.AutoGrowZeroInit requires configuring GrowColumnTo on MetaDBManager.")]
        public static partial void CapacityGrowHookMissing(ILogger logger);

        [LoggerMessage(2403, LogLevel.Error,
            "GrowColumnTo could not grow {Table}[{Col}] to {Rows} rows")]
        public static partial void CapacityGrowFailed(ILogger logger, string table, uint col, uint rows);

        [LoggerMessage(2404, LogLevel.Error,
            "EnsureRowCapacityProbe failed for {Table}[{Col}] needing {NeededRows} rows")]
        public static partial void EnsureRowCapacityProbeFailed(ILogger logger, string table, uint col, uint neededRows, Exception ex);

        // ───────── Run/RunAsync (2500-2599) ─────────

        [LoggerMessage(2500, LogLevel.Information,
            "Run {Operation} START")]
        public static partial void RunStart(ILogger logger, string operation);

        [LoggerMessage(2501, LogLevel.Information,
            "Run {Operation} END in {Ms} ms")]
        public static partial void RunEnd(ILogger logger, string operation, double ms);

        [LoggerMessage(2502, LogLevel.Error,
            "Run {Operation} FAILED in {Ms} ms")]
        public static partial void RunFail(ILogger logger, string operation, double ms, Exception ex);
    }
}
