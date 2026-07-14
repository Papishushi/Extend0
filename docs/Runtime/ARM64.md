# ARM64 release artifacts

Extend0 publishes the CLI as self-contained ARM64 artifacts for these runtime identifiers:

- `linux-arm64` on GitHub's native `ubuntu-24.04-arm` runner;
- `win-arm64` on the native `windows-11-arm` runner;
- `osx-arm64` on the native Apple Silicon `macos-15` runner.

The NuGet libraries remain architecture-neutral. Only the CLI application is published per runtime identifier.

## Validation contract

Every ARM64 job fails unless all of the following complete on the matching native architecture:

1. the self-contained `extend0` executable runs `doctor --json`;
2. the JSON version matches the project package version and reports `metadb_ready: true`;
3. the public `MetaDB.CreateManager()` API creates a mapped table;
4. a fresh manager reopens the table after the first manager closes it;
5. the mapped file and its persisted specification can be cleaned up.

Final archives include an SPDX JSON SBOM. Each archive has a sibling SHA-256 checksum and GitHub build-provenance and SBOM attestations. Workflow artifacts are named with the package version and RID and are immutable within their workflow run. A `v<package-version>` tag additionally publishes the same files as GitHub Release assets.

## Verification

After downloading an archive and its checksum:

```bash
sha256sum --check extend0-cli-<version>-<rid>.<archive>.sha256
gh attestation verify extend0-cli-<version>-<rid>.<archive> -R Papishushi/Extend0
```

Use `Get-FileHash -Algorithm SHA256` on Windows. Extract Unix archives with a tool that preserves executable permissions.

The Windows ARM runner is currently a GitHub public-preview image. Its workflow job is required: a generated `win-arm64` archive cannot pass the release gate without executing on that runner.
