#!/usr/bin/env python3
"""Compare two sets of masa-bench results and print a before/after table.

Input is a results directory holding files named

    <side>.<round>.<job>.txt

where <side> is "base" or "cand" and each file holds `key value` lines (the
stdout of the benchmarks, plus `perf.<job>.cycles` lines added by
scripts/bench.sh). Every metric is lower-is-better. Rounds of the same side
are repeated measurements of one metric.

The table reports, per metric, the minimum and median over rounds, the spread
((max - min) / min), the change of the minimum, and a verdict:

- "regression" / "improvement": the minimum moved by more than the threshold,
  and the median moved the same way (a lone lucky or unlucky round does not
  count). A metric that did not vary at all on either side (byte and
  allocation counts) is judged on any change.
- "noise": anything else.
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

DEFAULT_THRESHOLD_PCT = 3.0

Samples = dict[str, list[float]]


@dataclass(frozen=True)
class Summary:
    minimum: float
    median: float
    spread_pct: float


@dataclass(frozen=True)
class Row:
    key: str
    base: Summary | None
    cand: Summary | None
    delta_pct: float | None
    verdict: str


def parse_lines(text: str) -> dict[str, float]:
    """Parse `key value` lines, ignoring blanks, `#` comments and other text."""
    metrics: dict[str, float] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2 or parts[0].startswith("#"):
            continue
        try:
            metrics[parts[0]] = float(parts[1])
        except ValueError:
            continue
    return metrics


def load_results(directory: Path) -> dict[str, Samples]:
    """Read every `<side>.<round>.<job>.txt` file into side -> key -> samples."""
    sides: dict[str, Samples] = defaultdict(lambda: defaultdict(list))
    for path in sorted(directory.glob("*.txt")):
        side = path.name.split(".", 1)[0]
        for key, value in parse_lines(path.read_text(encoding="utf-8")).items():
            sides[side][key].append(value)
    return sides


def summarize(samples: list[float]) -> Summary:
    minimum = min(samples)
    spread = (max(samples) - minimum) / minimum * 100 if minimum else 0.0
    return Summary(minimum, statistics.median(samples), spread)


def pct_change(before: float, after: float) -> float:
    if before == 0:
        return 0.0 if after == 0 else float("inf")
    return (after - before) / before * 100


def judge(base: Summary, cand: Summary, threshold_pct: float) -> tuple[float, str]:
    """The change of the minimum in percent, and the verdict."""
    delta = pct_change(base.minimum, cand.minimum)
    median_delta = pct_change(base.median, cand.median)
    deterministic = base.spread_pct == 0 and cand.spread_pct == 0
    limit = 0.0 if deterministic else threshold_pct
    if delta > limit and median_delta > 0:
        return delta, "regression"
    if delta < -limit and median_delta < 0:
        return delta, "improvement"
    return delta, "noise"


def compare(
    base: Samples, cand: Samples, threshold_pct: float = DEFAULT_THRESHOLD_PCT
) -> list[Row]:
    rows: list[Row] = []
    for key in sorted(set(base) | set(cand)):
        base_summary = summarize(base[key]) if key in base else None
        cand_summary = summarize(cand[key]) if key in cand else None
        if base_summary is None:
            rows.append(Row(key, None, cand_summary, None, "new"))
        elif cand_summary is None:
            rows.append(Row(key, base_summary, None, None, "removed"))
        else:
            delta, verdict = judge(base_summary, cand_summary, threshold_pct)
            rows.append(Row(key, base_summary, cand_summary, delta, verdict))
    return rows


def format_number(value: float) -> str:
    return f"{value:.1f}" if abs(value) < 1000 else f"{value:.0f}"


def format_rows(rows: list[Row]) -> str:
    header = [
        "metric",
        "base min",
        "base med",
        "base spr%",
        "cand min",
        "cand med",
        "cand spr%",
        "delta%",
        "verdict",
    ]
    table = [header]
    for row in rows:
        cells = [row.key]
        for summary in (row.base, row.cand):
            if summary is None:
                cells += ["-", "-", "-"]
            else:
                cells += [
                    format_number(summary.minimum),
                    format_number(summary.median),
                    f"{summary.spread_pct:.1f}",
                ]
        cells.append("-" if row.delta_pct is None else f"{row.delta_pct:+.1f}")
        cells.append(row.verdict)
        table.append(cells)
    widths = [max(len(line[i]) for line in table) for i in range(len(header))]
    lines = []
    for index, line in enumerate(table):
        padded = [line[0].ljust(widths[0])]
        padded += [cell.rjust(widths[i]) for i, cell in enumerate(line[1:], start=1)]
        lines.append("  ".join(padded))
        if index == 0:
            lines.append("  ".join("-" * width for width in widths))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "results", type=Path, help="directory of <side>.<round>.<job>.txt"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD_PCT,
        help="percent change of the minimum that counts as real (default 3)",
    )
    parser.add_argument(
        "--filter", default="", help="only metrics whose key matches this regex"
    )
    args = parser.parse_args(argv)

    sides = load_results(args.results)
    if "base" not in sides or "cand" not in sides:
        print(
            f"{args.results}: need both base.* and cand.* result files",
            file=sys.stderr,
        )
        return 1
    rows = compare(sides["base"], sides["cand"], args.threshold)
    if args.filter:
        pattern = re.compile(args.filter)
        rows = [row for row in rows if pattern.search(row.key)]
    print(format_rows(rows))
    regressions = sum(row.verdict == "regression" for row in rows)
    improvements = sum(row.verdict == "improvement" for row in rows)
    print(
        f"\n{len(rows)} metrics: {regressions} regression, {improvements} improvement "
        f"(threshold {args.threshold:g}%, minimum over rounds)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
