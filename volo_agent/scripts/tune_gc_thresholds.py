from __future__ import annotations

import argparse
import os
import re
import statistics
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable


_ROW_RE = re.compile(
    r"^\|\s*(\d+)\s*\|\s*([^|]+?)\s*\|\s*(\d+)\s*\|\s*([0-9.]+)\s*\|\s*([0-9.]+)\s*\|\s*([0-9.]+)\s*\|"
)


@dataclass(frozen=True)
class CandidateResult:
    thresholds: tuple[int, int, int]
    total_p95_med: float
    total_p99_med: float
    exec_p95_med: float
    exec_p99_med: float
    total_p95_runs: tuple[float, ...]
    total_p99_runs: tuple[float, ...]
    exec_p95_runs: tuple[float, ...]
    exec_p99_runs: tuple[float, ...]


def _parse_candidate(raw: str) -> tuple[int, int, int]:
    parts = [p.strip() for p in str(raw).split(",")]
    if len(parts) != 3:
        raise ValueError(f"invalid threshold triplet: {raw}")
    vals = tuple(int(p) for p in parts)
    if any(v <= 0 for v in vals):
        raise ValueError(f"thresholds must be > 0: {raw}")
    return vals[0], vals[1], vals[2]


def _parse_candidates(raw: str) -> list[tuple[int, int, int]]:
    candidates: list[tuple[int, int, int]] = []
    for token in str(raw or "").split(";"):
        text = token.strip()
        if not text:
            continue
        candidates.append(_parse_candidate(text))
    deduped = sorted(set(candidates))
    if not deduped:
        raise ValueError("at least one candidate threshold triplet is required")
    return deduped


def _default_candidates() -> list[tuple[int, int, int]]:
    # Conservative search space around the current "safe" baseline.
    return [
        (1400, 20, 60),
        (1600, 20, 60),
        (1800, 25, 70),
        (2000, 30, 80),
        (2200, 30, 80),
        (2400, 30, 80),
        (2600, 35, 90),
        (2800, 40, 90),
        (3000, 45, 95),
        (3200, 45, 95),
        (3600, 50, 110),
        (4000, 60, 120),
    ]


def _run_once(
    *,
    iterations: int,
    warmup: int,
    concurrency: int,
    thresholds: tuple[int, int, int],
    enable_instrumentation: bool,
) -> dict[tuple[int, str], tuple[float, float]]:
    cmd = [
        sys.executable,
        "scripts/benchmark_request_paths.py",
        "--iterations",
        str(iterations),
        "--warmup",
        str(warmup),
        "--concurrency-levels",
        str(concurrency),
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = "."
    env["VOLO_GC_TUNING_MODE"] = "safe"
    env["VOLO_GC_THRESHOLDS"] = ",".join(str(v) for v in thresholds)
    env["VOLO_GC_PAUSE_INSTRUMENTATION"] = "1" if enable_instrumentation else "0"
    output = subprocess.check_output(cmd, text=True, env=env)

    parsed: dict[tuple[int, str], tuple[float, float]] = {}
    for line in output.splitlines():
        m = _ROW_RE.match(line.strip())
        if not m:
            continue
        conc = int(m.group(1))
        path = m.group(2).strip()
        p95 = float(m.group(5))
        p99 = float(m.group(6))
        parsed[(conc, path)] = (p95, p99)
    return parsed


def _median(values: Iterable[float]) -> float:
    return float(statistics.median(list(values)))


def _evaluate_candidate(
    *,
    thresholds: tuple[int, int, int],
    runs: int,
    iterations: int,
    warmup: int,
    concurrency: int,
    enable_instrumentation: bool,
) -> CandidateResult:
    total_p95_runs: list[float] = []
    total_p99_runs: list[float] = []
    exec_p95_runs: list[float] = []
    exec_p99_runs: list[float] = []

    for _ in range(runs):
        rows = _run_once(
            iterations=iterations,
            warmup=warmup,
            concurrency=concurrency,
            thresholds=thresholds,
            enable_instrumentation=enable_instrumentation,
        )
        t = rows[(concurrency, "flow.total")]
        e = rows[(concurrency, "flow.executor")]
        total_p95_runs.append(t[0])
        total_p99_runs.append(t[1])
        exec_p95_runs.append(e[0])
        exec_p99_runs.append(e[1])

    return CandidateResult(
        thresholds=thresholds,
        total_p95_med=_median(total_p95_runs),
        total_p99_med=_median(total_p99_runs),
        exec_p95_med=_median(exec_p95_runs),
        exec_p99_med=_median(exec_p99_runs),
        total_p95_runs=tuple(total_p95_runs),
        total_p99_runs=tuple(total_p99_runs),
        exec_p95_runs=tuple(exec_p95_runs),
        exec_p99_runs=tuple(exec_p99_runs),
    )


def _print_results(results: list[CandidateResult], *, top: int) -> None:
    ranked = sorted(
        results,
        key=lambda r: (r.total_p95_med, r.total_p99_med, r.exec_p95_med, r.exec_p99_med),
    )
    print("| Rank | Thresholds | total p95 med | total p99 med | exec p95 med | exec p99 med |")
    print("|---:|---|---:|---:|---:|---:|")
    for i, row in enumerate(ranked[: max(1, top)], start=1):
        thr = ",".join(str(v) for v in row.thresholds)
        print(
            f"| {i} | {thr} | {row.total_p95_med:.2f} | {row.total_p99_med:.2f} | "
            f"{row.exec_p95_med:.2f} | {row.exec_p99_med:.2f} |"
        )

    best = ranked[0]
    thr = ",".join(str(v) for v in best.thresholds)
    print()
    print("Best candidate")
    print(f"- thresholds: {thr}")
    print(
        f"- flow.total median p95/p99: {best.total_p95_med:.2f} / {best.total_p99_med:.2f} ms"
    )
    print(
        f"- flow.executor median p95/p99: {best.exec_p95_med:.2f} / {best.exec_p99_med:.2f} ms"
    )
    print(f"- total p95 runs: {list(best.total_p95_runs)}")
    print(f"- total p99 runs: {list(best.total_p99_runs)}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep GC thresholds for benchmark_request_paths.py and rank candidates "
            "by median flow.total p95/p99 at a target concurrency."
        )
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Benchmark runs per candidate (default: 3).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=200,
        help="Benchmark iterations per run (default: 200).",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=20,
        help="Benchmark warmup per run (default: 20).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Target concurrency level to optimize (default: 50).",
    )
    parser.add_argument(
        "--candidates",
        type=str,
        default="",
        help=(
            "Semicolon-separated threshold triplets, e.g. "
            "'2000,30,80;3000,45,95'. If omitted, uses built-in defaults."
        ),
    )
    parser.add_argument(
        "--top",
        type=int,
        default=8,
        help="How many ranked rows to print (default: 8).",
    )
    parser.add_argument(
        "--enable-instrumentation",
        action="store_true",
        help="Enable GC pause instrumentation during sweep (default: disabled).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.runs <= 0:
        raise ValueError("--runs must be > 0")
    if args.iterations <= 0 or args.warmup < 0:
        raise ValueError("--iterations must be > 0 and --warmup must be >= 0")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be > 0")

    candidates = (
        _parse_candidates(args.candidates) if args.candidates else _default_candidates()
    )
    print(
        f"Sweeping {len(candidates)} candidates at c={args.concurrency}, "
        f"runs={args.runs}, iterations={args.iterations}, warmup={args.warmup}."
    )
    print(
        f"GC instrumentation during sweep: "
        f"{'enabled' if args.enable_instrumentation else 'disabled'}"
    )

    results: list[CandidateResult] = []
    for idx, thresholds in enumerate(candidates, start=1):
        label = ",".join(str(v) for v in thresholds)
        print(f"[{idx}/{len(candidates)}] testing {label} ...")
        result = _evaluate_candidate(
            thresholds=thresholds,
            runs=args.runs,
            iterations=args.iterations,
            warmup=args.warmup,
            concurrency=args.concurrency,
            enable_instrumentation=args.enable_instrumentation,
        )
        print(
            f"  medians total p95/p99={result.total_p95_med:.2f}/{result.total_p99_med:.2f} ms "
            f"exec p95/p99={result.exec_p95_med:.2f}/{result.exec_p99_med:.2f} ms"
        )
        results.append(result)

    print()
    _print_results(results, top=args.top)


if __name__ == "__main__":
    main()
