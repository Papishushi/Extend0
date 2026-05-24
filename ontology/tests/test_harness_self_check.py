from __future__ import annotations

import unittest

from truth_questions import (
    TRUTH_QUESTIONS,
    check_abox_foundation_exists,
    check_readme_tells_platform_story,
    check_readme_uses_current_metadb_entry_surface,
    check_tbox_avoids_demo_core_leakage,
    check_tbox_keeps_core_platform_concepts,
    check_tbox_preserves_transport_abstraction,
    ensure_registry_complete,
    run_all_checks,
)


class TruthHarnessSelfCheckTests(unittest.TestCase):
    def test_registry_is_complete(self) -> None:
        ensure_registry_complete()
        self.assertGreaterEqual(len(TRUTH_QUESTIONS), 6)

    def test_readme_platform_check_can_fail(self) -> None:
        result = check_readme_tells_platform_story("Extend0 is a library.")
        self.assertFalse(result.passed)

    def test_metadb_surface_check_detects_old_markers(self) -> None:
        result = check_readme_uses_current_metadb_entry_surface("new MetaDBManager(); TryGetCreated();")
        self.assertFalse(result.passed)

    def test_demo_leakage_check_has_explicit_banlist(self) -> None:
        result = check_tbox_avoids_demo_core_leakage({"LifecycleSystem", "Cluster", "MetaDBSystem"})
        self.assertFalse(result.passed)

    def test_transport_check_returns_structured_result(self) -> None:
        result = check_tbox_preserves_transport_abstraction('<owl:Class rdf:about="#NamedPipeTransport"/>')
        self.assertFalse(result.passed)

    def test_abox_check_can_fail(self) -> None:
        result = check_abox_foundation_exists("@prefix sh: <http://www.w3.org/ns/shacl#> .", "@prefix ex: <https://extend0.se777en.fyi/abox#> .")
        self.assertFalse(result.passed)

    def test_individual_check_functions_return_results(self) -> None:
        checks = [
            check_readme_tells_platform_story(),
            check_readme_uses_current_metadb_entry_surface(),
            check_tbox_keeps_core_platform_concepts(),
            check_tbox_preserves_transport_abstraction(),
            check_abox_foundation_exists(),
        ]
        self.assertTrue(all(hasattr(result, "passed") for result in checks))

    def test_all_checks_return_results(self) -> None:
        self.assertTrue(all(hasattr(result, "detail") for result in run_all_checks()))


if __name__ == "__main__":
    unittest.main()
