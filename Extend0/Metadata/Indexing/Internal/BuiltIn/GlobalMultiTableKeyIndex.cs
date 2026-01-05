using Extend0.Metadata.Contract;
using Extend0.Metadata.Indexing.Definitions;
using System.Runtime.CompilerServices;
using static Extend0.Metadata.Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex;

namespace Extend0.Metadata.Indexing.Internal.BuiltIn;

/// <summary>
/// Built-in cross-table index that maps fixed-size UTF-8 (or binary) keys to a <c>(tableName, row, col)</c> location tuple.
/// </summary>
/// <remarks>
/// <para>
/// This index provides fast global key-based lookup across multiple metadata tables (partitions) that share a common
/// fixed-size key representation. It is optimized for rebuild workloads and low-allocation mutation patterns.
/// </para>
/// <para>
/// <b>Data structure</b>:
/// a two-level dictionary where the outer map is keyed by table id (<see cref="Guid"/>), and each inner map stores
/// key buffers (<see cref="byte"/>[]) mapped to <c>(tableName, row, col)</c>.
/// </para>
/// <para>
/// <b>Key normalization</b>:
/// lookups accept variable-length keys up to <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/> and normalize shorter keys to the fixed-size
/// form (zero-padded) using a thread-local scratch buffer, avoiding allocations.
/// </para>
/// <para>
/// <b>Pooling &amp; ownership</b>:
/// dictionary keys are stored as pooled fixed-size buffers rented from <see cref="IndexFixedSizeByteArrayPool"/>.
/// The index owns these buffers once inserted and returns them to the pool on removal/clear/rebuild cleanup.
/// </para>
/// <para>
/// <b>Last-wins semantics</b>:
/// when the same key is observed multiple times within a partition, the most recent value overwrites the previous one.
/// </para>
/// <para>
/// <b>Thread-safety</b>:
/// operations are synchronized via internal <see cref="ReaderWriterLockSlim"/> instances.
/// Lookups acquire a read lock; rebuild and mutation paths acquire a write lock.
/// </para>
/// <para>
/// This type supports both per-partition upserts (<see cref="Set"/>) and cross-partition updates (<see cref="SetAll"/>)
/// to enable merge/synchronization workflows.
/// </para>
/// </remarks>
internal sealed class GlobalMultiTableKeyIndex(string name, int tablesCapacity = 256, int perTableCapacity = 0, int keySize = 16)
    : CrossTableRebuildableIndexDefinition<byte[], Hit>(name, ByteArrayComparer.Ordinal, tablesCapacity, perTableCapacity, keySize)
{
    /// <summary>
    /// Attempts to retrieve the table location (row, column) associated with the specified key.
    /// </summary>
    /// <param name="key">The owned key buffer to query.</param>
    /// <param name="hit">When successful, receives the (row, column) location of the match.</param>
    /// <returns><see langword="true"/> if a hit was found; otherwise <see langword="false"/>.</returns>
    public bool TryGetHit(byte[] key, out Hit hit)
    {
        ThrowIfDisposed();
        if ((uint)key.Length > (uint)CachedKeySize)
        {
            hit = default;
            return false;
        }

        // Build lookup (fixed-size, padded) without allocation.
        var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);

        if (TryGetValue(lookup, out var a))
        {
            hit = a;
            return true;
        }
        hit = default;
        return false;
    }

    /// <summary>
    /// Attempts to retrieve the table location (row, column) associated with the specified UTF-8 span key.
    /// </summary>
    /// <param name="keyUtf8">A fixed-size UTF-8 span representing the key to query.</param>
    /// <param name="hit">When successful, receives the (row, column) location of the match.</param>
    /// <returns><see langword="true"/> if a hit was found; otherwise <see langword="false"/>.</returns>
    public bool TryGetHit(ReadOnlySpan<byte> keyUtf8, out Hit hit)
    {
        ThrowIfDisposed();
        if ((uint)keyUtf8.Length > (uint)CachedKeySize)
        {
            hit = default;
            return false;
        }

        // Build lookup (fixed-size, padded) without allocation.
        var scratch = GetScratchLookupKey(keyUtf8, CachedKeySize);

        if (TryGetValue(scratch, out var a))
        {
            hit = a;
            return true;
        }
        hit = default;
        return false;
    }

    /// <summary>
    /// Inserts or updates the location value for a key within a specific table partition.
    /// </summary>
    /// <param name="tableId">Target table identifier (partition key).</param>
    /// <param name="tableName">Logical table name to store alongside the location.</param>
    /// <param name="key">
    /// Key bytes to insert/update. The buffer may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>; in that case it is
    /// normalized for lookup by padding into a thread-local scratch buffer.
    /// If <paramref name="key"/> is empty or longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>, the method performs no work.
    /// </param>
    /// <param name="col">Column index to store.</param>
    /// <param name="row">Row index to store.</param>
    /// <remarks>
    /// <para>
    /// This method acquires a write lock and is safe to call concurrently with readers.
    /// </para>
    /// <para>
    /// If the key already exists in the target partition (comparison uses <see cref="ByteArrayComparer.Ordinal"/>),
    /// the stored value is updated in-place (last-wins semantics) without allocating or renting a pooled buffer.
    /// </para>
    /// <para>
    /// If the key does not exist, the method rents a fixed-size buffer from the internal pool, copies <paramref name="key"/>
    /// into it and zero-pads the remaining bytes, then stores that owned buffer as the dictionary key. From that point on,
    /// the index owns the stored key buffer and will return it to the pool on removal/clear.
    /// </para>
    /// <para>
    /// If an exception occurs after renting a pooled buffer, the buffer is returned to the pool to avoid leaks.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    internal void Set(Guid tableId, string tableName, byte[] key, uint col, uint row)
    {
        ThrowIfDisposed();

        if ((uint)key.Length > (uint)CachedKeySize || key.Length == 0)
            return;

        Rwls.EnterWriteLock();
        byte[]? owned = null;
        try
        {
            var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);

            if (!Index.TryGetValue(tableId, out var inner))
            {
                inner = new Dictionary<byte[], Hit>(ByteArrayComparer.Ordinal);
                Index[tableId] = inner;
            }

            if (inner.TryGetValue(lookup, out var existing))
            {
                inner[lookup] = new Hit(tableName, row, col, existing.OwnedKey);
                return;
            }

            if (!TryRentKey(key, out owned))
                return;

            inner.Add(owned, new Hit(tableName, row, col, owned));
        }
        catch
        {
            if (owned is not null)
                ReturnPooledKey(owned);
            throw;
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Inserts or updates the specified <paramref name="key"/> in <b>every</b> existing table partition,
    /// using <b>last-wins</b> semantics.
    /// </summary>
    /// <param name="tableName">Logical table name stored alongside the location tuple.</param>
    /// <param name="key">
    /// Key bytes identifying the entry to upsert. The key may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>; in that case it is
    /// normalized for lookup by padding into a thread-local scratch buffer.
    /// If <paramref name="key"/> is empty or longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>, the method performs no work.
    /// </param>
    /// <param name="col">Column index to store.</param>
    /// <param name="row">Row index to store.</param>
    /// <remarks>
    /// <para>
    /// This method acquires a write lock for the entire operation so readers never observe partially-updated state.
    /// </para>
    /// <para>
    /// The implementation snapshots the current set of table partitions <b>without allocating</b> by copying the current
    /// partition ids into a temporary buffer: <c>stackalloc</c> is used for small counts and <see cref="System.Buffers.ArrayPool{T}"/>
    /// is used for larger ones. This avoids invalidating the outer dictionary enumerator while allowing safe mutation of
    /// <see cref="CrossTableIndexBase{TInnerKey, TInnerValue}.Index"/> values.
    /// </para>
    /// <para>
    /// For each partition:
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///     If the key already exists (content-based comparison via <see cref="ByteArrayComparer.Ordinal"/>),
    ///     the value is updated in-place without renting a pooled buffer.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///     If the key does not exist, the method rents a fixed-size buffer from the internal pool, copies the key into it,
    ///     zero-pads the remaining bytes, and stores that owned buffer as the dictionary key. Ownership transfers to the index,
    ///     which will return it to the pool on removal/clear.
    ///     </description>
    ///   </item>
    /// </list>
    /// </para>
    /// <para>
    /// Any rented <see cref="Guid"/> buffer is always returned to the shared pool, even if an exception is thrown while
    /// applying updates.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    internal void SetAll(string tableName, byte[] key, uint col, uint row)
    {
        ThrowIfDisposed();

        if ((uint)key.Length > (uint)CachedKeySize || key.Length == 0)
            return;

        Rwls.EnterWriteLock();
        Guid[]? rented = null;
        try
        {
            var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);

            // Snapshot partition ids without GC allocations.
            var count = Index.Count;
            if (count == 0)
                return;

            const int STACK_THRESHOLD = 128; // Guid is 16 bytes => 128 ids ~= 2 KB stackalloc

            Span<Guid> tableIds = count <= STACK_THRESHOLD
                ? stackalloc Guid[count]
                : (rented = System.Buffers.ArrayPool<Guid>.Shared.Rent(count));

            var n = 0;
            foreach (var kvp in Index)
                tableIds[n++] = kvp.Key;

            // Apply to snapshot (safe to mutate Index now).
            for (var i = 0; i < n; i++)
            {
                var tableId = tableIds[i];

                if (!Index.TryGetValue(tableId, out var inner) || inner is null)
                {
                    inner = new Dictionary<byte[], Hit>(ByteArrayComparer.Ordinal);
                    Index[tableId] = inner;
                }

                // Update fast-path (no rent)
                if (inner.TryGetValue(lookup, out var existing))
                {
                    inner[lookup] = new Hit(tableName, row, col, existing.OwnedKey);
                    continue;
                }

                // Insert path (rent per partition)
                byte[]? owned = null;
                try
                {
                    if (!TryRentKey(key, out owned))
                        continue;

                    inner.Add(owned, new Hit(tableName, row, col, owned));
                    owned = null; // ownership transferred to dictionary
                }
                finally
                {
                    if (owned is not null)
                        ReturnPooledKey(owned);
                }
            }
        }
        finally
        {
            if (rented is not null)
                System.Buffers.ArrayPool<Guid>.Shared.Return(rented, clearArray: false);

            Rwls.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes an entire table partition from the index.
    /// </summary>
    /// <param name="key">The table identifier (partition key) to remove.</param>
    /// <returns>
    /// <see langword="true"/> if the partition existed and was removed; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This operation is performed under a write lock to prevent concurrent readers from observing
    /// partially-cleared state.
    /// </para>
    /// <para>
    /// If the partition stores pooled <see cref="byte"/>[] keys (typical for fixed-size binary indexes),
    /// all key buffers currently held by that partition are returned to the internal pool before the
    /// partition is removed.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override bool Remove(Guid key)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (!Index.TryGetValue(key, out var tableMap))
                return false;

            foreach (var k in tableMap.Keys)
                ReturnPooledKey(k);

            return Index.Remove(key);
        }
        finally
        {
            Rwls.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes a key from a specific table partition.
    /// </summary>
    /// <param name="tableId">The target table partition identifier.</param>
    /// <param name="key">The key to remove.</param>
    /// <returns>
    /// <see langword="true"/> if the key existed in that partition and was removed; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This operation is performed under a write lock.
    /// </para>
    /// <para>
    /// When the partition stores pooled fixed-size <see cref="byte"/>[] keys, the caller may provide an
    /// equivalent key buffer (same bytes, different reference). In that case, this method performs a
    /// content-based search using <see cref="CrossTableIndexBase{TInnerKey, TInnerValue}.InnerKeyComparer"/> to locate the actual stored key instance,
    /// removes that entry, and returns the stored pooled buffer to the pool.
    /// </para>
    /// <para>
    /// For non-<see cref="byte"/>[] key types (or when keys are not pooled), this method performs a normal
    /// dictionary removal using the provided <paramref name="key"/>.
    /// </para>
    /// <para>
    /// If the partition becomes empty after removal, the partition itself is removed from the outer map.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override bool Remove(Guid tableId, byte[] key)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if (!Index.TryGetValue(tableId, out var tableMap))
                return false;

            if ((uint)key.Length > (uint)CachedKeySize)
                return false;

            var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);

            if (!tableMap.TryGetValue(lookup, out var hit))
                return false;

            if (!tableMap.Remove(hit.OwnedKey))
                return false;

            ReturnPooledKey(hit.OwnedKey);

            if (tableMap.Count == 0)
                Index.Remove(tableId);

            return true;
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Removes a key from the first table partition that contains it.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns>
    /// <see langword="true"/> if the key was found and removed; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This operation is performed under a write lock.
    /// </para>
    /// <para>
    /// If the key exists in multiple partitions, only the first match (by outer map enumeration order)
    /// is removed.
    /// </para>
    /// <para>
    /// When partitions store pooled fixed-size <see cref="byte"/>[] keys, the caller may provide an
    /// equivalent buffer (same bytes, different reference). In that case, this method searches by content
    /// using <see cref="CrossTableIndexBase{TInnerKey, TInnerValue}.InnerKeyComparer"/>, removes the stored entry, and returns the stored pooled buffer
    /// to the pool.
    /// </para>
    /// <para>
    /// If the partition becomes empty after removal, the partition itself is removed from the outer map.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    public override bool Remove(byte[] key)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        try
        {
            if ((uint)key.Length > (uint)CachedKeySize)
                return false;

            var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);

            foreach (var kv in Index)
            {
                var tableId = kv.Key;
                var tableMap = kv.Value;

                if (!tableMap.TryGetValue(lookup, out var hit))
                    continue;

                if (!tableMap.Remove(hit.OwnedKey))
                    return false;

                ReturnPooledKey(hit.OwnedKey);

                if (tableMap.Count == 0)
                    Index.Remove(tableId);

                return true; // first match wins
            }

            return false;
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Gets the set of table partitions that currently contain the specified key.
    /// </summary>
    /// <param name="key">
    /// Key bytes to search for. The buffer may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>; in that case the key is
    /// normalized to the fixed-size representation (zero-padded) using a thread-local scratch buffer before the lookup.
    /// If <paramref name="key"/> is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>, the method returns an empty array.
    /// </param>
    /// <returns>
    /// An array of table identifiers (<see cref="Guid"/>) for partitions that contain the key; or <see cref="Array.Empty{T}"/>
    /// when the key is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/> or no partitions match.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This overload accepts variable-length keys for convenience and avoids allocations by using a thread-local scratch buffer
    /// when padding is required.
    /// </para>
    /// <para>
    /// The returned array is produced by the base implementation and represents the current membership at the time of the call.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0301", Justification = "Hot-path: explicit Array.Empty<T>() usage.")]
    public override Guid[] GetMemberTables(byte[] key)
    {
        ThrowIfDisposed();

        if ((uint)key.Length > (uint)CachedKeySize)
            return Array.Empty<Guid>();

        var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);
        return base.GetMemberTables(lookup);
    }

    /// <summary>
    /// Gets the set of table partitions that currently contain the specified key and optionally returns the associated values
    /// found in those partitions.
    /// </summary>
    /// <param name="key">
    /// Key bytes to search for. The buffer may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>; in that case the key is normalized
    /// to the fixed-size representation (zero-padded) using a thread-local scratch buffer before the lookup.
    /// If <paramref name="key"/> is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>, the method returns an empty array and
    /// sets <paramref name="value"/> to <see langword="default"/>.
    /// </param>
    /// <param name="value">
    /// When this method returns, contains an array of values corresponding to the returned table ids (same ordering),
    /// or <see langword="null"/>/<see langword="default"/> if the key is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/> or no matches exist.
    /// </param>
    /// <returns>
    /// An array of table identifiers (<see cref="Guid"/>) for partitions that contain the key; or <see cref="Array.Empty{T}"/>
    /// when the key is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/> or no partitions match.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This overload is optimized for lookups where both membership and values are required, and avoids allocations by using a
    /// thread-local scratch buffer when padding is needed.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0301", Justification = "Hot-path: explicit Array.Empty<T>() usage.")]
    public override Guid[] GetMemberTables(byte[] key, out Hit[]? value)
    {
        ThrowIfDisposed();

        if ((uint)key.Length > (uint)CachedKeySize)
        {
            value = default;
            return Array.Empty<Guid>();
        }

        var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);
        return base.GetMemberTables(lookup, out value);
    }

    /// <summary>
    /// Attempts to get the value associated with the specified key, searching across all table partitions.
    /// </summary>
    /// <param name="key">
    /// Key bytes to search for. The buffer may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>; in that case the key is normalized
    /// to the fixed-size representation (zero-padded) using a thread-local scratch buffer before the lookup.
    /// If <paramref name="key"/> is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>, the method returns <see langword="false"/>.
    /// </param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, contains the associated value; otherwise, <see langword="default"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the key exists in at least one partition; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This overload accepts variable-length keys for convenience and pads shorter keys using a thread-local scratch buffer
    /// to avoid allocations.
    /// </remarks>
    public override bool TryGetValue(byte[] key, out Hit value)
    {
        ThrowIfDisposed();

        if ((uint)key.Length > (uint)CachedKeySize)
        {
            value = default;
            return false;
        }

        var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);
        return base.TryGetValue(lookup, out value);
    }

    /// <summary>
    /// Attempts to get the value associated with the specified key inside a specific table partition.
    /// </summary>
    /// <param name="tableId">The table partition identifier.</param>
    /// <param name="key">
    /// Key bytes to search for. The buffer may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>; in that case the key is normalized
    /// to the fixed-size representation (zero-padded) using a thread-local scratch buffer before the lookup.
    /// If <paramref name="key"/> is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>, the method returns <see langword="false"/>.
    /// </param>
    /// <param name="value">
    /// When this method returns <see langword="true"/>, contains the associated value; otherwise, <see langword="default"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the key exists in the specified partition; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// This overload accepts variable-length keys for convenience and pads shorter keys using a thread-local scratch buffer
    /// to avoid allocations.
    /// </remarks>
    public override bool TryGetValue(Guid tableId, byte[] key, out Hit value)
    {
        ThrowIfDisposed();

        if ((uint)key.Length > (uint)CachedKeySize)
        {
            value = default;
            return false;
        }

        var lookup = key.Length == CachedKeySize ? key : GetScratchLookupKey(key, CachedKeySize);
        return base.TryGetValue(tableId, lookup, out value);
    }

    /// <summary>
    /// Adds an entry using a fixed-size binary key by materializing an owned pooled key buffer.
    /// </summary>
    /// <param name="key">
    /// Input key bytes. The key may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>; in that case it is normalized
    /// by copying into a pooled buffer and zero-padding the remaining bytes. If the key is longer than
    /// <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>, the operation fails and returns <see langword="false"/>.
    /// </param>
    /// <param name="value">The value to associate with the normalized key.</param>
    /// <param name="tableId">
    /// Optional target table partition. When provided, the entry is added into that specific partition;
    /// otherwise, the entry is added using the cross-table add semantics of the base implementation.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the entry was added successfully; otherwise <see langword="false"/> (e.g. key already existed
    /// per the base implementation semantics, or the key was invalid/too long to be normalized).
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method centralizes the pooling/ownership rules for writes:
    /// it always rents a fixed-size buffer from the internal pool and copies the provided <paramref name="key"/> into it,
    /// ensuring stable dictionary key semantics (fixed-size, deterministic padding).
    /// </para>
    /// <para>
    /// On success, ownership of the rented buffer transfers to the index (it becomes part of the dictionary keys).
    /// On failure (duplicate / not inserted) or if an exception occurs, the buffer is returned to the pool to avoid leaks.
    /// </para>
    /// <para>
    /// This method assumes the caller enforces any additional invariants (such as requiring exact-sized keys for writes).
    /// It does not acquire locks itself; the underlying <c>base.Add</c> methods are expected to provide the required synchronization,
    /// or callers should ensure proper locking at a higher level.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool AddFixed(byte[] key, Hit value, Guid? tableId = null)
    {
        ThrowIfDisposed();

        // Rent a pooled owned buffer and copy key into it.
        if (!TryRentKey(key, out var owned))
            return false;

        try
        {
            // IMPORTANT: stored Hit must reference the actual dictionary key instance.
            var stored = new Hit(value.TableName, value.Row, value.Col, owned);

            var ok = tableId.HasValue
                ? base.Add(tableId.Value, owned, stored)
                : base.Add(owned, stored);

            if (ok)
                return true;

            // Not inserted => give buffer back.
            ReturnPooledKey(owned);
            return false;
        }
        catch
        {
            // Exception => never leak pooled buffer.
            ReturnPooledKey(owned);
            throw;
        }
    }

    /// <summary>
    /// Adds a new entry into the index by normalizing <paramref name="key"/> into a pooled fixed-size owned buffer.
    /// </summary>
    /// <param name="key">Key bytes to insert (normalized to <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/> via pooling and zero-padding).</param>
    /// <param name="value">The value to associate with the key.</param>
    /// <returns><see langword="true"/> if the entry was added; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// This overload delegates to <see cref="AddFixed(byte[], ValueTuple{string, uint, uint}, Guid?)"/> to ensure
    /// consistent pooling and cleanup semantics.
    /// </remarks>
    public override bool Add(byte[] key, Hit value) => AddFixed(key, value);

    /// <summary>
    /// Adds a new entry into a specific table partition by normalizing <paramref name="key"/> into a pooled fixed-size owned buffer.
    /// </summary>
    /// <param name="tableId">The target table partition identifier.</param>
    /// <param name="key">Key bytes to insert (normalized to <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/> via pooling and zero-padding).</param>
    /// <param name="value">The value to associate with the key.</param>
    /// <returns><see langword="true"/> if the entry was added; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// This overload delegates to <see cref="AddFixed(byte[], ValueTuple{string, uint, uint}, Guid?)"/> and passes
    /// <paramref name="tableId"/> so the insertion targets a single partition while preserving pooling/ownership rules.
    /// </remarks>
    public override bool Add(Guid tableId, byte[] key, Hit value) => AddFixed(key, value, tableId);

    /// <summary>
    /// Adds a complete table partition to the index by <b>copying</b> all incoming keys into pooled,
    /// fixed-size owned buffers.
    /// </summary>
    /// <param name="key">The table partition identifier.</param>
    /// <param name="value">
    /// Source partition map. Keys may be shorter than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/> (they will be zero-padded),
    /// but must not be longer. The index does <b>not</b> take ownership of these key buffers.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the partition was added; otherwise <see langword="false"/> if the partition already existed.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This overload is designed for safety: it prevents external mutation/reuse of key buffers from corrupting
    /// the index by always renting pooled buffers and copying keys into them.
    /// </para>
    /// <para>
    /// On success, the index owns the pooled key buffers and will recycle them on <c>Remove/Clear/ClearTable</c>.
    /// On failure or exception, all rented buffers are returned to the pool.
    /// </para>
    /// <para>
    /// Duplicate keys inside <paramref name="value"/> follow <b>last-wins</b> semantics.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Thrown if the index has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Thrown if any key is longer than <see cref="CrossTableRebuildableIndexDefinition{TInnerKey, TInnerValue}.CachedKeySize"/>.</exception>
    public override bool Add(Guid key, IDictionary<byte[], Hit> value)
    {
        ThrowIfDisposed();
        Rwls.EnterWriteLock();
        Dictionary<byte[], Hit>? ownedMap = null;
        try
        {
            // If partition already exists, do not allocate/rent anything.
            if (Index.ContainsKey(key))
                return false;

            ownedMap = new Dictionary<byte[], Hit>(value.Count, ByteArrayComparer.Ordinal);

            foreach (var kv in value)
            {
                var srcKey = kv.Key;

                if ((uint)srcKey.Length > (uint)CachedKeySize)
                    throw new InvalidOperationException($"Invalid key length in partition. KeySize={srcKey.Length}, Max={CachedKeySize}.");

                // Rent + copy + zero-pad (shorter keys OK, empty keys are skipped by TryRentKey).
                if (!TryRentKey(srcKey, out var ownedKey))
                    continue;

                // Last-wins but do not leak rented buffers:
                // if key already exists (by content), update the existing entry and return the newly rented key.
                if (ownedMap.TryGetValue(ownedKey, out var prev))
                {
                    ownedMap[ownedKey] = new Hit(kv.Value.TableName, kv.Value.Row, kv.Value.Col, prev.OwnedKey);
                    ReturnPooledKey(ownedKey);
                }
                else ownedMap.Add(ownedKey, new Hit(kv.Value.TableName, kv.Value.Row, kv.Value.Col, ownedKey));
            }

            // Attach partition atomically.
            Index[key] = ownedMap;
            return true;
        }
        catch
        {
            if (ownedMap is not null)
                foreach (var k in ownedMap.Keys)
                    ReturnPooledKey(k);
            throw;
        }
        finally
        {
            Rwls.ExitWriteLock();
        }
    }

    /// <summary>
    /// Rebuilds the entire index by scanning all registered tables in the manager.
    /// </summary>
    /// <param name="manager">The database manager providing access to all metadata tables.</param>
    public override void Rebuild(IMetaDBManager manager)
    {
        ThrowIfDisposed();

        Rwls.EnterWriteLock();
        try
        {
            foreach (var tableId in manager.TableIds)
            {
                if (!manager.TryGetManaged(tableId, out var table))
                    continue;

                if (!Index.TryGetValue(tableId, out var existing))
                {
                    existing = new Dictionary<byte[], Hit>(ByteArrayComparer.Ordinal);
                    Index[tableId] = existing;
                }
                else
                {
                    foreach (var key in existing.Keys)
                        ReturnPooledKey(key);
                    existing.Clear();
                }

                foreach (var entry in table.EnumerateCells())
                {
                    if (!TryRentKey(entry, out var owned)) continue;
                    if (existing.TryGetValue(owned, out var prev))
                    {
                        existing[owned] = new Hit(table.Spec.Name, entry.Row, entry.Col, prev.OwnedKey);
                        ReturnPooledKey(owned);
                    }
                    else existing.Add(owned, new Hit(table.Spec.Name, entry.Row, entry.Col, owned));
                }
            }
        }
        finally { Rwls.ExitWriteLock(); }
    }

    /// <summary>
    /// Immutable location record for a resolved key hit in a cross-table index.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="Hit"/> represents the logical destination of a key lookup:
    /// the owning table name plus the cell coordinates (<see cref="Row"/>, <see cref="Col"/>).
    /// </para>
    /// <para>
    /// <b>Ownership invariant</b>:
    /// <see cref="OwnedKey"/> must reference the <b>exact</b> key buffer instance stored as the dictionary key
    /// inside the index (i.e., the pooled fixed-size key array owned by the index).
    /// This is critical for correct removals and for returning pooled buffers to the pool without leaks.
    /// </para>
    /// <para>
    /// Consumers should treat <see cref="OwnedKey"/> as an internal implementation detail:
    /// it is not intended for external mutation or long-term retention.
    /// </para>
    /// </remarks>
    internal readonly struct Hit
    {
        /// <summary>
        /// Logical name of the table that contains the hit.
        /// </summary>
        public readonly string TableName;

        /// <summary>
        /// Row index of the hit within <see cref="TableName"/>.
        /// </summary>
        public readonly uint Row;

        /// <summary>
        /// Column index of the hit within <see cref="TableName"/>.
        /// </summary>
        public readonly uint Col;

        /// <summary>
        /// The owned pooled key buffer instance that is actually stored as the dictionary key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This must be the same reference as the key used in the underlying dictionary entry.
        /// It enables removal by reference and correct buffer return to the pool.
        /// </para>
        /// <para>
        /// Internal by design: do not expose publicly to prevent misuse (mutation/retention).
        /// </para>
        /// </remarks>
        internal readonly byte[] OwnedKey;

        /// <summary>
        /// Creates a new hit record.
        /// </summary>
        /// <param name="tableName">Logical name of the table that contains the hit.</param>
        /// <param name="row">Row index of the hit.</param>
        /// <param name="col">Column index of the hit.</param>
        /// <param name="ownedKey">
        /// The owned pooled key buffer instance stored as the dictionary key for this entry.
        /// Must not be a temporary/scratch buffer.
        /// </param>
        public Hit(string tableName, uint row, uint col, byte[] ownedKey)
            => (TableName, Row, Col, OwnedKey) = (tableName, row, col, ownedKey);
    }
}