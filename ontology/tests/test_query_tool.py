from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SKILL_DIR = ROOT / "ontology" / "skills" / "ontology-query"
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from ontology_tools import class_json, classes_json, individuals_json, sparql_json  # noqa: E402


class OntologyQueryToolTests(unittest.TestCase):
    def test_classes_output_contains_core_class(self) -> None:
        payload = json.loads(classes_json())
        self.assertTrue(any(entry["id"] == "ns:LifecycleSystem" for entry in payload["classes"]))

    def test_class_output_contains_related_properties(self) -> None:
        payload = json.loads(class_json("MetaDBSystem"))
        self.assertIn("ns:usesManager", payload["relatedProperties"])

    def test_individuals_output_contains_example_access_surface(self) -> None:
        payload = json.loads(individuals_json())
        self.assertTrue(any(entry["id"] == "ex:metadb-access" for entry in payload["individuals"]))

    def test_sparql_subset_returns_rows(self) -> None:
        payload = json.loads(sparql_json("SELECT ?c WHERE { ?c rdf:type owl:Class . }"))
        self.assertIn("rows", payload)
        self.assertTrue(any(row["c"] == "ns:MetaDBSystem" for row in payload["rows"]))


if __name__ == "__main__":
    unittest.main()
