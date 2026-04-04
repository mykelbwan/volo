from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_script_module():
    script_path = (
        Path(__file__).resolve().parents[3] / "scripts" / "zombie_code_audit.py"
    )
    spec = importlib.util.spec_from_file_location("zombie_code_audit", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_scan_repo_flags_tests_only_and_unused_but_not_live(tmp_path):
    mod = _load_script_module()

    (tmp_path / "pkg").mkdir()
    (tmp_path / "tests").mkdir()

    (tmp_path / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "pkg" / "helpers.py").write_text(
        "\n".join(
            [
                "def live():",
                "    return helper()",
                "",
                "def helper():",
                "    return 1",
                "",
                "def test_only():",
                "    return 2",
                "",
                "def unused():",
                "    return 3",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "pkg" / "consumer.py").write_text(
        "\n".join(
            [
                "from pkg.helpers import live",
                "",
                "VALUE = live()",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "tests" / "test_helpers.py").write_text(
        "\n".join(
            [
                "from pkg.helpers import test_only",
                "",
                "def test_it():",
                "    assert test_only() == 2",
            ]
        ),
        encoding="utf-8",
    )

    candidates = mod.scan_repo(tmp_path)
    by_symbol = {candidate.symbol: candidate for candidate in candidates}

    assert "pkg.helpers.live" not in by_symbol
    assert "pkg.helpers.helper" not in by_symbol
    assert by_symbol["pkg.helpers.test_only"].status == "tests_only"
    assert by_symbol["pkg.helpers.unused"].status == "unused"
