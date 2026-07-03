"""relay_sdk のパッケージング（pyproject.toml）に関する健全性チェック。"""
from __future__ import annotations

import tomllib
from pathlib import Path


def _load_pyproject() -> dict:
    path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _dep_name(spec: str) -> str:
    for sep in (">=", "==", "<=", "~=", "!=", ">", "<"):
        if sep in spec:
            return spec.split(sep, 1)[0].strip()
    return spec.strip()


class TestHttpxRuntimeDependency:
    def test_httpx_is_declared_as_runtime_dependency(self):
        """medium5: relay_sdk は import 時に httpx が必須なので runtime dependency
        として宣言する（dev group のみだと relay_sdk を単体 install したアプリで
        ImportError になる）。"""
        pyproject = _load_pyproject()
        deps = pyproject["project"]["dependencies"]
        names = {_dep_name(dep) for dep in deps}
        assert "httpx" in names, f"httpx が [project.dependencies] に見つかりません: {deps}"

    def test_httpx_still_importable_for_dev_tests(self):
        # runtime dependency 化しても既存の統合テスト（LiveServer 等）が壊れていないこと。
        import httpx  # noqa: F401
