from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path


def build_version_text(version: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return "\n".join(
        [
            "VSVersionInfo(",
            "  ffi=FixedFileInfo(",
            "    filevers=(0, 1, 0, 0),",
            "    prodvers=(0, 1, 0, 0),",
            "    mask=0x3f,",
            "    flags=0x0,",
            "    OS=0x40004,",
            "    fileType=0x1,",
            "    subtype=0x0,",
            "    date=(0, 0)",
            "  ),",
            "  kids=[",
            "    StringFileInfo([",
            "      StringTable(",
            "        '040904B0',",
            "        [",
            f"          StringStruct('FileVersion', '{version}'),",
            f"          StringStruct('ProductVersion', '{version}'),",
            "          StringStruct('ProductName', 'Extend0 Ontology Diagnostics'),",
            "          StringStruct('InternalName', 'abox-doctor'),",
            f"          StringStruct('Comments', 'Generated at {timestamp}')",
            "        ]",
            "      )",
            "    ]),",
            "    VarFileInfo([VarStruct('Translation', [1033, 1200])])",
            "  ]",
            ")",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a Windows version-info resource file for abox-doctor packaging.")
    parser.add_argument("--version", default="0.1.0", help="Semantic version string.")
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent / "version_info.txt"), help="Output path.")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.write_text(build_version_text(args.version), encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
