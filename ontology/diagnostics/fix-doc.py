from __future__ import annotations

import importlib.util
import json
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
ABOX_DOCTOR_PATH = CURRENT_DIR / "abox-doctor.py"


def _load_abox_doctor_module():
    spec = importlib.util.spec_from_file_location("extend0_abox_doctor", ABOX_DOCTOR_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load abox-doctor.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    doctor = _load_abox_doctor_module()
    payload = doctor.run_doctor([str(Path(__file__).resolve().parents[1] / "abox" / "example-abox.ttl")])
    print(json.dumps(doctor.emit_fix_doc(payload), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
