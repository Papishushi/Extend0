from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[2]
README_PATH = ROOT / "README.md"
TBOX_PATH = ROOT / "ontology" / "tbox" / "extend0.owl"
ABOX_SCHEMA_PATH = ROOT / "ontology" / "abox" / "abox-schema.ttl"
EXAMPLE_ABOX_PATH = ROOT / "ontology" / "abox" / "example-abox.ttl"

OWL_NS = {"owl": "http://www.w3.org/2002/07/owl#", "rdfs": "http://www.w3.org/2000/01/rdf-schema#"}


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    question: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class TruthQuestion:
    check_id: str
    question: str
    evaluator: Callable[[], CheckResult]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _tbox_tree() -> ET.Element:
    return ET.fromstring(_read_text(TBOX_PATH))


def _owl_classes() -> set[str]:
    root = _tbox_tree()
    classes: set[str] = set()
    for node in root.findall("owl:Class", OWL_NS):
        about = node.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "")
        if about.startswith("#"):
            classes.add(about[1:])
    return classes


def _contains_all(text: str, expected: list[str]) -> list[str]:
    return [item for item in expected if item not in text]


def check_readme_tells_platform_story(text: str | None = None) -> CheckResult:
    text = text if text is not None else _read_text(README_PATH)
    missing = _contains_all(
        text,
        ["platform", "Lifecycle", "MetaDB", "ontology", "code generation"],
    )
    return CheckResult(
        check_id="readme-platform-story",
        question="Does the README describe Extend0 as a platform of cooperating systems?",
        passed=not missing,
        detail="missing: " + ", ".join(missing) if missing else "README tells the major 1 platform story.",
    )


def check_readme_uses_current_metadb_entry_surface(text: str | None = None) -> CheckResult:
    text = text if text is not None else _read_text(README_PATH)
    forbidden = [marker for marker in ["new MetaDBManager(", "TryGetCreated("] if marker in text]
    required = _contains_all(text, ["MetaDBManagerSingleton", "IMetaDBManagerRPCCompatible"])
    passed = not forbidden and not required
    detail_parts: list[str] = []
    if forbidden:
        detail_parts.append("forbidden markers: " + ", ".join(forbidden))
    if required:
        detail_parts.append("missing markers: " + ", ".join(required))
    if not detail_parts:
        detail_parts.append("README uses the current public MetaDB access surface.")
    return CheckResult(
        check_id="readme-metadb-surface",
        question="Does the README align MetaDB guidance with the current public access surface?",
        passed=passed,
        detail="; ".join(detail_parts),
    )


def check_tbox_keeps_core_platform_concepts(classes: set[str] | None = None) -> CheckResult:
    classes = classes if classes is not None else _owl_classes()
    missing = sorted(
        {
            "Extend0Platform",
            "LifecycleSystem",
            "MetaDBSystem",
            "Transport",
            "ServiceIdentity",
            "AccessSurface",
            "OwnershipClaim",
            "Lease",
            "HeartbeatSignal",
        }
        - classes
    )
    return CheckResult(
        check_id="tbox-core-concepts",
        question="Does the TBox keep the accepted core platform concepts?",
        passed=not missing,
        detail="missing classes: " + ", ".join(missing) if missing else "Core platform concepts are present.",
    )


def check_tbox_avoids_demo_core_leakage(classes: set[str] | None = None) -> CheckResult:
    classes = classes if classes is not None else _owl_classes()
    banned = sorted(classes.intersection({"Cluster", "Node", "ServiceRegistration", "ClientSession", "ClusterRegistryTable", "HeartbeatRecord"}))
    return CheckResult(
        check_id="tbox-no-demo-leakage",
        question="Does the TBox avoid promoting demo-specific concepts into the core model?",
        passed=not banned,
        detail="banned classes still present: " + ", ".join(banned) if banned else "No banned demo-derived concepts are present in the core TBox.",
    )


def check_tbox_preserves_transport_abstraction(text: str | None = None) -> CheckResult:
    text = text if text is not None else _read_text(TBOX_PATH)
    required = _contains_all(
        text,
        [
            'rdf:about="#Transport"',
            'rdf:about="#NamedPipeTransport"',
            'rdf:about="#TcpTransport"',
            'rdf:about="#usesTransport"',
        ],
    )
    return CheckResult(
        check_id="tbox-transport-abstraction",
        question="Does the TBox preserve transport as an abstraction with named pipe as one implementation?",
        passed=not required,
        detail="missing markers: " + ", ".join(required) if required else "Transport abstraction is explicitly modeled.",
    )


def check_abox_foundation_exists(schema_text: str | None = None, example_text: str | None = None) -> CheckResult:
    schema_text = schema_text if schema_text is not None else _read_text(ABOX_SCHEMA_PATH)
    example_text = example_text if example_text is not None else _read_text(EXAMPLE_ABOX_PATH)
    required = []
    for marker in ["sh:NodeShape", "ns:Extend0Platform", "ns:AccessSurface"]:
        if marker not in schema_text:
            required.append(f"schema:{marker}")
    for marker in ["ex:extend0-platform", "ex:lifecycle-system", "ex:metadb-system", "ex:metadb-access"]:
        if marker not in example_text:
            required.append(f"example:{marker}")
    return CheckResult(
        check_id="abox-foundation",
        question="Do the ABox schema and example graph exist for the current platform core?",
        passed=not required,
        detail="missing markers: " + ", ".join(required) if required else "ABox schema and example graph are present.",
    )


TRUTH_QUESTIONS = [
    TruthQuestion("readme-platform-story", "Does the README describe Extend0 as a platform of cooperating systems?", check_readme_tells_platform_story),
    TruthQuestion("readme-metadb-surface", "Does the README align MetaDB guidance with the current public access surface?", check_readme_uses_current_metadb_entry_surface),
    TruthQuestion("tbox-core-concepts", "Does the TBox keep the accepted core platform concepts?", check_tbox_keeps_core_platform_concepts),
    TruthQuestion("tbox-no-demo-leakage", "Does the TBox avoid promoting demo-specific concepts into the core model?", check_tbox_avoids_demo_core_leakage),
    TruthQuestion("tbox-transport-abstraction", "Does the TBox preserve transport as an abstraction with named pipe as one implementation?", check_tbox_preserves_transport_abstraction),
    TruthQuestion("abox-foundation", "Do the ABox schema and example graph exist for the current platform core?", check_abox_foundation_exists),
]


def run_all_checks() -> list[CheckResult]:
    return [question.evaluator() for question in TRUTH_QUESTIONS]


def ensure_registry_complete() -> None:
    expected = {
        "check_readme_tells_platform_story",
        "check_readme_uses_current_metadb_entry_surface",
        "check_tbox_keeps_core_platform_concepts",
        "check_tbox_avoids_demo_core_leakage",
        "check_tbox_preserves_transport_abstraction",
        "check_abox_foundation_exists",
    }
    actual = {question.evaluator.__name__ for question in TRUTH_QUESTIONS}
    if expected != actual:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise RuntimeError(f"truth-question registry mismatch; missing={missing}; extra={extra}")


def main() -> int:
    ensure_registry_complete()
    results = run_all_checks()
    payload = {
        "passed": all(result.passed for result in results),
        "results": [result.__dict__ for result in results],
    }
    print(json.dumps(payload, indent=2))
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
