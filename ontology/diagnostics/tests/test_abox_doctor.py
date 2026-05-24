from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
DIAGNOSTICS_DIR = CURRENT_DIR.parent
ABOX_DOCTOR_PATH = DIAGNOSTICS_DIR / "abox-doctor.py"


def load_abox_doctor():
    spec = importlib.util.spec_from_file_location("extend0_abox_doctor_test", ABOX_DOCTOR_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load abox-doctor.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ABoxDoctorTests(unittest.TestCase):
    def test_doctor_payload_has_expected_shape(self) -> None:
        doctor = load_abox_doctor()
        payload = doctor.run_doctor([str(Path(__file__).resolve().parents[2] / "abox" / "example-abox.ttl")])
        self.assertEqual(payload["tool"], "extend0-abox-doctor")
        self.assertIn("stats", payload)
        self.assertIn("findings", payload)
        self.assertIn("unsupportedOperations", payload)
        self.assertGreater(payload["stats"]["classCount"], 0)

    def test_fix_doc_projection_keeps_schema_fields(self) -> None:
        doctor = load_abox_doctor()
        payload = doctor.run_doctor([str(Path(__file__).resolve().parents[2] / "abox" / "example-abox.ttl")])
        fix_doc = doctor.emit_fix_doc(payload)
        self.assertEqual(set(fix_doc.keys()), {"tool", "generatedAtUtc", "graphPaths", "findings", "unsupportedOperations"})
        self.assertTrue(all("supportedFix" in finding for finding in fix_doc["findings"]))


if __name__ == "__main__":
    unittest.main()
