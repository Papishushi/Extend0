# Extend0 Test Bug Tracker

This file tracks confirmed product bugs discovered while expanding automated test coverage.

## Active Bugs

- None currently tracked.

## Fixed Bugs

- 2026-06-08
  - failing test name: `Extend0.Tests.Metadata.CodeGen.MetadataEntryGeneratorExtensibilityTests.Generator_EmitsConsumerDeclaredCustomShape`
  - affected file or subsystem: `Extend0.MetadataEntry.Generator.MetadataEntryGenerator.EmitVariant`
  - symptom: generated `MetadataEntry{K}x{V}` structs used `ReadOnlySpan<byte>` and `Span<byte>` but did not emit `using System;`, so consumer projects or direct generator-driver compilations without implicit usings failed to compile custom generated shapes.
  - fix: the variant template now emits `using System;`, making generated entry structs self-contained regardless of the consumer project's implicit-usings setting.
  - regression test: `Extend0.Tests.Metadata.CodeGen.MetadataEntryGeneratorExtensibilityTests.Generator_EmitsConsumerDeclaredCustomShape`
  - status: fixed

- 2026-06-08
  - failing test name: `Extend0.Tests.Metadata.Indexing.Internal.BuiltIn.BuiltInIndexTests.GlobalMultiTableKeyIndex_RebuildScansCreatedTablesAndSkipsLazyTables`
  - affected file or subsystem: `Extend0.Metadata.Indexing.Internal.BuiltIn.GlobalMultiTableKeyIndex.Rebuild`
  - symptom: manager-level cross-table rebuild repopulated created table partitions but did not clear stale partitions that were no longer present or no longer materialized, allowing obsolete global-key hits to survive a rebuild.
  - fix: `GlobalMultiTableKeyIndex.Rebuild` now returns all existing pooled keys, clears every partition, and then rebuilds only currently managed/materialized tables from the manager.
  - regression test: `Extend0.Tests.Metadata.Indexing.Internal.BuiltIn.BuiltInIndexTests.GlobalMultiTableKeyIndex_RebuildScansCreatedTablesAndSkipsLazyTables`
  - status: fixed

- 2026-06-08
  - failing test name: `Extend0.Tests.Metadata.MetaDBManagerBehaviorTests.RunHelpers_ValidateInputs_AndLogFailures`
  - affected file or subsystem: `Extend0.Metadata.MetaDBManager` pipeline runner wrappers
  - symptom: `Run("name", null!)` and related reindex runner wrappers could invoke a captured null user callback and surface `NullReferenceException` instead of the documented `ArgumentNullException`.
  - fix: `Run`, `RunAsync`, `RunWithReindexAll`, `RunWithReindexAllAsync`, `RunWithReindexTable`, and `RunWithReindexTableAsync` now validate `operationName` and `action` before wrapping or executing user callbacks.
  - regression test: `Extend0.Tests.Metadata.MetaDBManagerBehaviorTests.RunHelpers_ValidateInputs_AndLogFailures`
  - status: fixed

- 2026-06-08
  - failing test name: `Extend0.Tests.Lifecycle.CrossProcess.CrossProcessOrchestratorTests.GetOrStart_WhenServerFactoryThrows_DisposesCreatedOwnerService`
  - affected file or subsystem: `Extend0.Lifecycle.CrossProcess.CrossProcessOrchestrator<TService>`
  - symptom: when a process won ownership and the service factory succeeded, but owner-side transport host creation failed, the orchestrator released the mutex/CTS/server state but leaked the newly-created owner service implementation.
  - fix: owner-branch failure cleanup now cancels hosting, disposes the partial server host, disposes the created service implementation best-effort, then releases and disposes the ownership mutex.
  - regression test: `Extend0.Tests.Lifecycle.CrossProcess.CrossProcessOrchestratorTests.GetOrStart_WhenServerFactoryThrows_DisposesCreatedOwnerService`
  - status: fixed

- 2026-06-08
  - failing test name: `Extend0.Tests.Lifecycle.CrossProcess.CrossProcessSingletonTests.FailedInitialization_DoesNotPoisonSingletonRegistry`
  - affected file or subsystem: `Extend0.Lifecycle.CrossProcess.CrossProcessSingleton<TService>`
  - symptom: if static initialization failed after the base `Singleton` constructor registered the instance, the in-process singleton registry kept a partially constructed `CrossProcessSingleton<TService>` with no usable static `Service`, blocking later initialization when `Overwrite=false`.
  - fix: `CrossProcessSingleton<TService>` now rolls back the base singleton registration by disposing itself when `InitializeStatic` throws, then rethrows the original initialization failure.
  - regression test: `Extend0.Tests.Lifecycle.CrossProcess.CrossProcessSingletonTests.FailedInitialization_DoesNotPoisonSingletonRegistry`
  - status: fixed

- 2026-06-08
  - failing test name: `Extend0.Tests.Metadata.Indexing.Internal.BuiltIn.BuiltInIndexTests.ColumnKeyIndex_CoversInvalidInputAndSpanLookupBranches` (removed from committed assertions after confirmation)
  - affected file or subsystem: `Extend0.Metadata.Indexing.Internal.BuiltIn.ColumnKeyIndex.RemoveExact`
  - symptom: `RemoveExact` is documented and named as requiring the exact stored key buffer instance, but the underlying dictionary uses `ByteArrayComparer.Ordinal`; passing a different byte array with equivalent contents can remove the entry.
  - fix: `RemoveExact` now verifies `ReferenceEquals` against stored dictionary keys before removing.
  - regression test: `Extend0.Tests.Metadata.Indexing.Internal.BuiltIn.BuiltInIndexTests.ColumnKeyIndex_CoversInvalidInputAndSpanLookupBranches`
  - status: fixed

## Notes

- Test infrastructure now uses `Extend0.Testing` as the only assembly with internal visibility.
- Add one entry per confirmed code bug with:
  - date
  - failing test name
  - affected file or subsystem
  - symptom
  - status
