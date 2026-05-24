from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
SKILL_DIR = CURRENT_DIR.parent / "skills" / "ontology-query"
TESTS_DIR = CURRENT_DIR.parent / "tests"
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from ontology_tools import DEFAULT_ABOX_PATH, load_graph  # noqa: E402
from truth_questions import run_all_checks  # noqa: E402


FIX_DOC_SCHEMA_PATH = CURRENT_DIR / "fix-doc-schema.json"


def build_finding(check_result: object) -> dict:
    severity = "info" if getattr(check_result, "passed") else "warning"
    return {
        "id": getattr(check_result, "check_id"),
        "severity": severity,
        "summary": getattr(check_result, "question"),
        "evidence": [getattr(check_result, "detail")],
        "recommendedAction": "No action required." if getattr(check_result, "passed") else "Review the evidence and align docs, ontology, or examples.",
        "supportedFix": False,
    }


def run_doctor(abox_paths: list[str]) -> dict:
    graph = load_graph(abox_paths)
    checks = run_all_checks()
    findings = [build_finding(check) for check in checks]

    if not graph.individuals:
        findings.append(
            {
                "id": "abox-empty",
                "severity": "error",
                "summary": "The loaded ABox graph contains no individuals.",
                "evidence": ["No individuals were discovered in the loaded ABox files."],
                "recommendedAction": "Provide at least one ABox example or operational instance graph.",
                "supportedFix": False,
            }
        )

    payload = {
        "tool": "extend0-abox-doctor",
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "graphPaths": [str(Path(path)) for path in abox_paths],
        "stats": {
            "classCount": len(graph.classes),
            "propertyCount": len(graph.properties),
            "individualCount": len(graph.individuals),
            "tripleCount": len(graph.triples),
        },
        "findings": findings,
        "unsupportedOperations": [
            "automatic ontology mutation",
            "full SHACL validation",
            "full RDF/SPARQL engine repair suggestions",
        ],
    }
    return payload


def emit_fix_doc(payload: dict) -> dict:
    return {
        "tool": payload["tool"],
        "generatedAtUtc": payload["generatedAtUtc"],
        "graphPaths": payload["graphPaths"],
        "findings": payload["findings"],
        "unsupportedOperations": payload["unsupportedOperations"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a minimal diagnostic pass over the Extend0 ontology foundation.")
    parser.add_argument("--abox", action="append", default=[str(DEFAULT_ABOX_PATH)], help="ABox Turtle file to inspect.")
    parser.add_argument("--emit-fix-doc", action="store_true", help="Emit a fix-document skeleton instead of the full diagnostic payload.")
    parser.add_argument("--output", help="Optional path for the JSON output.")
    args = parser.parse_args()

    payload = run_doctor(args.abox)
    document = emit_fix_doc(payload) if args.emit_fix_doc else payload
    text = json.dumps(document, indent=2, sort_keys=True)

    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
