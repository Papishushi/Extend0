from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SKILL_DIR = ROOT / "skills" / "ontology-query"
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from ontology_tools import slugify  # noqa: E402


class SlugFixtureTests(unittest.TestCase):
    def test_slug_fixture_examples(self) -> None:
        fixture_path = ROOT / "abox" / "slug-fixture.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        for example in fixture["examples"]:
            self.assertEqual(slugify(example["input"]), example["slug"])


if __name__ == "__main__":
    unittest.main()
