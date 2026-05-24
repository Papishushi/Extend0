# PyInstaller spec scaffold for the Extend0 ontology doctor.
# This file is intentionally minimal and should be treated as a packaging starting point,
# not as a fully validated release recipe.

from pathlib import Path


project_dir = Path.cwd()
doctor_path = project_dir / "ontology" / "diagnostics" / "abox-doctor.py"
version_info_path = project_dir / "ontology" / "diagnostics" / "version_info.txt"


a = Analysis(
    [str(doctor_path)],
    pathex=[str(project_dir)],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="abox-doctor",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    version=str(version_info_path) if version_info_path.exists() else None,
)
