#!/usr/bin/env python3
"""VibeSys evaluator shim for Masa's Hotel Reservation app.

Both VibeSys gates call this file, which wraps Masa's experiment runner:

- ``accuracy`` checks that the candidate changed only the agent-owned policy
  slots, builds and tests those slots with the task's features, and runs Hotel
  at light load, where nearly every request must succeed within its SLO.
- ``benchmark`` runs Hotel through ``exp_runner`` over a Poisson load sweep
  past saturation, computes goodput from the load generator's per-request CSVs,
  and reports it with the VibeSys evaluator result protocol (version 2,
  ``vs_protocol.py``).

Inputs (experiment configs and the build features) live next to this file,
under the task directory, which coding agents cannot edit. The script copies a
config into ``exp/hotel/in/<name>/`` (git-ignored) because ``exp_runner`` reads
experiments from there.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import fcntl
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from vs_protocol import OUTPUT_FLAG, MetricSpec, ProtocolReport

TASK_DIR = Path(__file__).resolve().parent.parent
EXPERIMENTS_DIR = TASK_DIR / "benchmark" / "experiments"

#: Cargo features every Hotel service is built with. ``sched_slo`` gives the
#: deadline-ordered baseline; ``stack_custom`` and ``sched_custom`` route the
#: policy stack and the run queue through the two agent-owned slots.
FEATURES = "sched_slo,stack_custom,sched_custom"

#: The Masa commit the task was authored against. The scope check compares the
#: candidate with it.
BASELINE_COMMIT = "f88d533c3b1242181814ac68b5a4736d1c552901"

#: Files a candidate may change. Everything else that exists at the baseline
#: commit is evaluator, framework or application code and must stay unchanged.
ALLOWED_PREFIXES = ("libs/masa-policy/src/agent/",)
ALLOWED_FILES = (
    "libs/rpcstack-sched/src/custom.rs",
    "libs/rpcstack-sched/src/replay_tests.rs",
)

#: Integration tests that pin the slot wiring: `DefaultHooks` is the agent
#: stack, responses carry a context, and the runtime uses the custom queue.
SLOT_TESTS = ("custom_stack_serve", "custom_sched_serve")

#: Serializes runs: exp_runner uses fixed container names and image tags, so
#: two candidates must not deploy Hotel at the same time on one Docker host.
LOCK_PATH = Path("/tmp/vibesys-masa-hotel.lock")

SUCCESS = "/None"

#: Light-load gate: the fraction of offered requests that must succeed within
#: their SLO at the lowest load point, overall and per API. It rejects
#: policies that buy overload goodput by shedding or aborting work they could
#: have served.
LIGHT_LOAD_MIN_GOODPUT_FRACTION = 0.97

#: Failed requests (any non-OK status, including timeouts, rejections and
#: aborts) tolerated in the accuracy run, as a fraction of offered requests.
ACCURACY_MAX_ERROR_FRACTION = 0.01


class EvaluationError(RuntimeError):
    """The candidate could not be evaluated, with a reason for the agent."""


@dataclass(frozen=True)
class Point:
    """One load point of an experiment, aggregated over its APIs."""

    rps: int
    offered: int
    good: int
    errors: int
    duration_s: float
    good_by_api: dict[str, tuple[int, int]]
    success_latencies_us: list[int]

    @property
    def goodput_rps(self) -> float:
        return self.good / self.duration_s

    @property
    def goodput_fraction(self) -> float:
        return self.good / self.offered if self.offered else 0.0


def tool(name: str) -> str:
    """Resolve a toolchain binary, also checking the default per-user install dirs."""
    found = shutil.which(name)
    if found:
        return found
    for directory in (
        Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "bin",
        Path.home() / ".local" / "bin",
    ):
        candidate = directory / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise EvaluationError(f"`{name}` is not on PATH; the task needs it installed on the host")


def environment(extra_env: dict[str, str] | None = None) -> dict[str, str]:
    """Environment for child commands: toolchain dirs on PATH, protoc located."""
    env = dict(os.environ) | (extra_env or {})
    extra = [str(Path(tool("cargo")).parent), str(Path(tool("uv")).parent)]
    env["PATH"] = os.pathsep.join([*extra, env.get("PATH", "")])
    if "PROTOC" not in env:
        protoc = shutil.which("protoc", path=env["PATH"]) or str(
            Path.home() / ".local" / "protoc" / "bin" / "protoc"
        )
        if Path(protoc).is_file():
            env["PROTOC"] = protoc
    return env


def run(
    argv: list[str],
    root: Path,
    *,
    timeout: float,
    log: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> None:
    """Run one command in the candidate root, failing with the output tail."""
    print(f"$ {' '.join(argv)}", flush=True)
    started = time.monotonic()
    result = subprocess.run(
        argv,
        cwd=root,
        env=environment(extra_env),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )
    if log is not None:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(result.stdout)
    print(f"  exit {result.returncode} after {time.monotonic() - started:.0f}s", flush=True)
    if result.returncode != 0:
        tail = "\n".join(result.stdout.splitlines()[-60:])
        raise EvaluationError(f"`{' '.join(argv)}` exited {result.returncode}:\n{tail}")


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=True, check=True
    ).stdout


def allowed(path: str) -> bool:
    return path.startswith(ALLOWED_PREFIXES) or path in ALLOWED_FILES


def check_scope(root: Path) -> None:
    """Fail when the candidate changed a file outside the agent-owned slots.

    Only paths under top-level entries that exist at the baseline commit are
    checked, so files VibeSys itself adds at the repository root are ignored.
    """
    try:
        git(root, "cat-file", "-e", f"{BASELINE_COMMIT}^{{commit}}")
    except subprocess.CalledProcessError as error:
        raise EvaluationError(
            f"baseline commit {BASELINE_COMMIT} is not in this repository's history"
        ) from error
    top_level = set(git(root, "ls-tree", "--name-only", BASELINE_COMMIT).split())
    top_level.discard(".vibesys")
    changed = set(git(root, "diff", "--name-only", BASELINE_COMMIT).split())
    changed |= set(git(root, "ls-files", "--others", "--exclude-standard").split())
    violations = sorted(
        path for path in changed if path.split("/", 1)[0] in top_level and not allowed(path)
    )
    if violations:
        listed = "\n".join(f"  {path}" for path in violations)
        raise EvaluationError(
            "changes outside the agent-owned slots (libs/masa-policy/src/agent/, "
            "libs/rpcstack-sched/src/custom.rs, libs/rpcstack-sched/src/replay_tests.rs):\n"
            f"{listed}\nRevert them; the rest of the repository is evaluator and framework code."
        )


@contextlib.contextmanager
def hotel_lock():
    LOCK_PATH.touch(exist_ok=True)
    with LOCK_PATH.open("r+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_experiment(root: Path, kind: str, *, timeout: float) -> tuple[Path, dict]:
    """Run one task-owned experiment config through exp_runner.

    Returns the output directory of the single policy and the generator config.
    """
    name = f"vibesys_{kind}"
    source = EXPERIMENTS_DIR / kind
    target = root / "exp" / "hotel" / "in" / name
    shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(source, target)
    (target / "policies").write_text(FEATURES + "\n")
    out = root / "exp" / "hotel" / "out" / name
    shutil.rmtree(out, ignore_errors=True)
    logs = root / "exp" / "hotel" / "vibesys-logs"
    with hotel_lock():
        run(
            [tool("uv"), "run", "--quiet", "-m", "exp_runner", "run", "hotel", name, "--rm-data"],
            root,
            timeout=timeout,
            log=logs / f"{name}.log",
        )
    policy_dirs = [path for path in (out / "0").iterdir() if path.is_dir()]
    if len(policy_dirs) != 1:
        raise EvaluationError(f"expected one policy output directory under {out / '0'}")
    return policy_dirs[0], json.loads((source / "gen_config.json").read_text())


def read_points(policy_dir: Path, gen_config: dict) -> list[Point]:
    """Aggregate the load generator's per-request CSVs into one Point per rate."""
    duration_s = float(gen_config["DurationSecs"])
    points = []
    for rps in gen_config["Rps"]:
        offered = good = errors = 0
        good_by_api: dict[str, tuple[int, int]] = {}
        latencies: list[int] = []
        for api in gen_config["Apis"]:
            path = policy_dir / f"r{rps}_{api}.csv"
            if not path.is_file():
                raise EvaluationError(f"load generator wrote no results for {api} at {rps} rps")
            api_offered = api_good = 0
            with path.open(newline="") as handle:
                for row in csv.DictReader(handle):
                    api_offered += 1
                    latency = int(row["latency"])
                    if row["error"] != SUCCESS:
                        errors += 1
                    elif latency <= int(row["slo"]):
                        api_good += 1
                        latencies.append(latency)
            offered += api_offered
            good += api_good
            good_by_api[api] = (api_good, api_offered)
        if offered == 0:
            raise EvaluationError(f"no requests were recorded at {rps} rps")
        points.append(Point(rps, offered, good, errors, duration_s, good_by_api, latencies))
    return points


def check_light_load(point: Point) -> None:
    """Fail when the lightest load point does not serve nearly everything in SLO."""
    shortfalls = [
        f"{api}: {good}/{offered}"
        for api, (good, offered) in point.good_by_api.items()
        if offered and good / offered < LIGHT_LOAD_MIN_GOODPUT_FRACTION
    ]
    if point.goodput_fraction < LIGHT_LOAD_MIN_GOODPUT_FRACTION or shortfalls:
        raise EvaluationError(
            f"at {point.rps} rps (below saturation) only {point.good}/{point.offered} requests "
            f"succeeded within their SLO ({point.goodput_fraction:.3f}; required "
            f">= {LIGHT_LOAD_MIN_GOODPUT_FRACTION}); per API: "
            + ", ".join(f"{api}: {g}/{o}" for api, (g, o) in point.good_by_api.items())
        )


def percentile(values: list[int], q: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    return float(ordered[min(len(ordered) - 1, int(q * len(ordered)))])


def accuracy(root: Path) -> None:
    check_scope(root)
    print("scope: ok", flush=True)
    # Masa's own checks deny warnings (scripts/check.sh).
    deny_warnings = {"RUSTFLAGS": "-D warnings"}
    cargo = tool("cargo")
    tests = [arg for name in SLOT_TESTS for arg in ("--test", name)]
    for argv in (
        [cargo, "check", "--quiet", "--features", FEATURES],
        [cargo, "test", "--quiet", "-p", "rpcstack-sched", "--features", "sched_custom"],
        [cargo, "test", "--quiet", "-p", "masa-integration-tests", "--features", FEATURES, *tests],
    ):
        run(argv, root, timeout=1800, extra_env=deny_warnings)
    policy_dir, gen_config = run_experiment(root, "accuracy", timeout=2400)
    points = read_points(policy_dir, gen_config)
    for point in points:
        if point.errors > ACCURACY_MAX_ERROR_FRACTION * point.offered:
            raise EvaluationError(
                f"{point.errors}/{point.offered} requests failed at {point.rps} rps (light load); "
                "a policy must not reject or abort requests it can serve"
            )
        check_light_load(point)
        print(f"light load {point.rps} rps: {point.good}/{point.offered} within SLO", flush=True)


METRICS = {
    "mean_goodput_rps": MetricSpec(unit="req/s", direction="max"),
    "mean_goodput_fraction": MetricSpec(unit="fraction", direction="max"),
    "peak_load_goodput_rps": MetricSpec(unit="req/s", direction="max"),
    "light_load_goodput_fraction": MetricSpec(unit="fraction", direction="max"),
    "error_fraction": MetricSpec(unit="fraction", direction="min"),
    "peak_load_p99_success_latency_ms": MetricSpec(unit="ms", direction="min", required=False),
}


def benchmark(root: Path, report: ProtocolReport) -> None:
    report.declare(METRICS)
    check_scope(root)
    policy_dir, gen_config = run_experiment(root, "benchmark", timeout=3000)
    points = read_points(policy_dir, gen_config)
    check_light_load(points[0])
    for point in points:
        per_api = ", ".join(
            f"{api} {good}/{offered}" for api, (good, offered) in point.good_by_api.items()
        )
        print(
            f"{point.rps:>5} rps: goodput {point.goodput_rps:8.1f} req/s "
            f"({point.goodput_fraction:.3f} of offered; errors {point.errors}; {per_api})",
            flush=True,
        )
    offered = sum(point.offered for point in points)
    report.emit(
        {
            "mean_goodput_rps": sum(p.goodput_rps for p in points) / len(points),
            "mean_goodput_fraction": sum(p.goodput_fraction for p in points) / len(points),
            "peak_load_goodput_rps": points[-1].goodput_rps,
            "light_load_goodput_fraction": points[0].goodput_fraction,
            "error_fraction": sum(p.errors for p in points) / offered,
            "peak_load_p99_success_latency_ms": percentile(points[-1].success_latencies_us, 0.99)
            / 1000,
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=("accuracy", "benchmark"))
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument(OUTPUT_FLAG, dest="vs_output", type=Path, default=None)
    args = parser.parse_args()
    root = args.project_root.resolve()
    if args.mode == "accuracy":
        try:
            accuracy(root)
        except (EvaluationError, subprocess.TimeoutExpired) as error:
            print(f"accuracy: FAIL\n{error}", file=sys.stderr)
            return 1
        print("accuracy: PASS")
        return 0
    with ProtocolReport(args.vs_output) as report:
        try:
            benchmark(root, report)
        except (EvaluationError, subprocess.TimeoutExpired) as error:
            print(f"benchmark: FAIL\n{error}", file=sys.stderr)
            report.fail(str(error))
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
