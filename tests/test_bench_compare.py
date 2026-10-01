from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "bench_compare.py"
SPEC = importlib.util.spec_from_file_location("bench_compare", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
bench_compare = importlib.util.module_from_spec(SPEC)
sys.modules["bench_compare"] = bench_compare
SPEC.loader.exec_module(bench_compare)


def verdicts(
    base: dict[str, list[float]], cand: dict[str, list[float]]
) -> dict[str, str]:
    return {row.key: row.verdict for row in bench_compare.compare(base, cand)}


def test_parse_lines_keeps_only_key_value_pairs() -> None:
    text = (
        "# comment\nhook_cost.x.total_ns 12.5\nnot a metric line\nbad value\nk nan?\n"
    )
    assert bench_compare.parse_lines(text) == {"hook_cost.x.total_ns": 12.5}


def test_regression_needs_min_and_median_to_move() -> None:
    result = verdicts(
        {"slower": [100, 104, 110], "lucky_round": [100, 104, 110]},
        {"slower": [120, 125, 130], "lucky_round": [120, 90, 91]},
    )
    assert result["slower"] == "regression"
    assert result["lucky_round"] == "improvement"


def test_small_changes_are_noise() -> None:
    assert verdicts({"m": [100, 101, 102]}, {"m": [101, 102, 103]}) == {"m": "noise"}


def test_improvement() -> None:
    assert verdicts({"m": [100, 101, 102]}, {"m": [80, 81, 82]}) == {"m": "improvement"}


def test_deterministic_metric_is_judged_on_any_change() -> None:
    result = verdicts({"allocs": [34, 34, 34]}, {"allocs": [35, 35, 35]})
    assert result == {"allocs": "regression"}


def test_metric_on_one_side_only() -> None:
    result = verdicts({"old": [1.0]}, {"new": [1.0]})
    assert result == {"old": "removed", "new": "new"}


def test_load_results_and_main_print_table(tmp_path: Path, capsys) -> None:
    for round_number, (base, cand) in enumerate([(100, 150), (102, 151)]):
        (tmp_path / f"base.{round_number}.job.txt").write_text(f"a.b {base}\n")
        (tmp_path / f"cand.{round_number}.job.txt").write_text(f"a.b {cand}\n")
    assert bench_compare.main([str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "a.b" in out
    assert "regression" in out
    assert "+50.0" in out


def test_main_requires_both_sides(tmp_path: Path) -> None:
    (tmp_path / "base.0.job.txt").write_text("a.b 1\n")
    assert bench_compare.main([str(tmp_path)]) == 1
