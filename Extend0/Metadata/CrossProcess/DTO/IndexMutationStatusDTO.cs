namespace Extend0.Metadata.CrossProcess.DTO;

/// <summary>
/// Status codes returned by index mutation operations (add/remove/replace).
/// </summary>
public enum IndexMutationStatusDTO : byte
{
    /// <summary>Operation completed successfully.</summary>
    Ok = 0,

    /// <summary>An index with the specified name already exists and replacement was not requested.</summary>
    AlreadyExists = 1,

    /// <summary>The requested index could not be found (typically on remove or replace).</summary>
    NotFound = 2,

    /// <summary>The target table is not open and cannot be mutated.</summary>
    TableNotOpen = 10,

    /// <summary>The provided index name is invalid (null/empty/whitespace or fails validation rules).</summary>
    InvalidName = 11,

    /// <summary>The provided index kind is invalid or unknown to the receiver.</summary>
    InvalidKind = 12,

    /// <summary>
    /// The request referenced one or more columns that do not exist or are incompatible with the index kind.
    /// </summary>
    InvalidColumn = 13,

    /// <summary>The operation is not supported by the current backend/version/configuration.</summary>
    NotSupported = 14,

    /// <summary>The requested index is built-in/protected and cannot be modified.</summary>
    BuiltInProtected = 15,

    /// <summary>An unexpected error occurred while processing the operation.</summary>
    Error = 255
}