namespace Extend0.Metadata.Contract;

/// <summary>
/// Represents a successful hit returned by a global lookup within a single table,
/// identifying the exact cell coordinates where the match was found.
/// </summary>
/// <param name="Col">The column index of the matching cell within the table.</param>
/// <param name="Row">The row index of the matching cell within the table.</param>
/// <remarks>
/// This value is scoped to one table instance (no cross-table semantics). It is commonly used by
/// <c>TryFindGlobal</c>-style APIs to replace tuple returns and keep the result extensible.
/// </remarks>
public readonly record struct TryFindGlobalHit(uint Col, uint Row);
