#!/usr/bin/env python3
"""
benchmark_small_query_parallel.py
===================================
Benchmarks parallel small-query annotation jobs dispatched via Celery.

What it tests
-------------
- Throughput: how many small-query annotation tasks complete per second
- Latency:    per-task wall-clock time (submit → done)
- Concurrency: behaviour at different worker_concurrency levels
- Contention: remote ClickHouse behaviour under simultaneous sessions

Usage
-----
  # 1. Start Celery workers (adjust -c for concurrency)
  celery -A celery_app worker -c 8 --loglevel=info -Q celery

  # 2. Run benchmark
  python benchmark_small_query_parallel.py \
      --concurrency 8 \
      --tasks 32 \
      --variant-count 500 \
      --genome hg19 \
      --scores-table nc_spark.scores_hg19_normalized \
      --gene-table nc_spark.nearest_gene_hg19 \
      --timeout 300

  # 3. Sweep concurrency levels automatically
  python benchmark_small_query_parallel.py --sweep
"""

import argparse
import csv
import json
import os
import random
import statistics
import string
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# Optional: rich progress bar — falls back to plain print if not installed
# ---------------------------------------------------------------------------
try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    _RICH = True
    console = Console()
except ImportError:
    _RICH = False
    console = None  # type: ignore

# ---------------------------------------------------------------------------
# Celery app import — same broker/backend as production
# ---------------------------------------------------------------------------
try:
    from celery_app import celery_app
    from celery import group, chord
except ImportError as e:
    sys.exit(
        f"Could not import celery_app: {e}\n"
        "Run this script from the project root where celery_app.py lives."
    )

# ---------------------------------------------------------------------------
# Benchmark task — a thin wrapper around materialise_annotations
# that accepts pre-built variants directly (no DB upload needed)
# so we can benchmark the ClickHouse query path in isolation.
# ---------------------------------------------------------------------------
try:
    from app.api.annotation import materialise_annotations  # the real task
except ImportError:
    materialise_annotations = None


# ───────────────────────────────────────────────────────────────────────────
# Synthetic variant generator
# ───────────────────────────────────────────────────────────────────────────

CHROMS = [str(i) for i in range(1, 23)] + ["X", "Y"]
BASES = ["A", "C", "G", "T"]


def _random_base(exclude: str) -> str:
    return random.choice([b for b in BASES if b != exclude])


def generate_variants(n: int, seed: int = 42) -> list[tuple]:
    """
    Generate n realistic-looking (chr, pos, ref, alt) variant tuples.
    Positions are sampled from ranges typical of coding regions to maximise
    JOIN hits against the scores table.
    """
    rng = random.Random(seed)
    variants = []
    for _ in range(n):
        chrom = rng.choice(CHROMS)
        pos = rng.randint(100_000, 250_000_000)
        ref = rng.choice(BASES)
        alt = _random_base(ref)
        variants.append((chrom, pos, ref, alt))
    return variants


# ───────────────────────────────────────────────────────────────────────────
# Session-ID generator (mirrors production format)
# ───────────────────────────────────────────────────────────────────────────


def _new_session_id(prefix: str = "bench") -> str:
    short = uuid.uuid4().hex[:8]
    return f"{prefix}_{short}"


# ───────────────────────────────────────────────────────────────────────────
# Lightweight mock task for dry-run / CI mode (no real ClickHouse needed)
# ───────────────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="benchmark.mock_annotate")
def mock_annotate_task(self, session_id: str, variant_count: int, sleep_ms: int = 200):
    """
    Simulates annotation latency without touching ClickHouse.
    Useful for benchmarking pure Celery/Redis overhead.
    """
    time.sleep(sleep_ms / 1000)
    return {
        "session_id": session_id,
        "rows_annotated": variant_count,
        "status": "complete",
    }


# ───────────────────────────────────────────────────────────────────────────
# Result dataclass
# ───────────────────────────────────────────────────────────────────────────


@dataclass
class TaskResult:
    session_id: str
    variant_count: int
    submit_ts: float
    start_ts: Optional[float] = None
    end_ts: Optional[float] = None
    success: bool = False
    error: Optional[str] = None
    rows_annotated: int = 0

    @property
    def queue_latency(self) -> Optional[float]:
        if self.start_ts and self.submit_ts:
            return self.start_ts - self.submit_ts
        return None

    @property
    def execution_time(self) -> Optional[float]:
        if self.start_ts and self.end_ts:
            return self.end_ts - self.start_ts
        return None

    @property
    def total_latency(self) -> Optional[float]:
        if self.end_ts:
            return self.end_ts - self.submit_ts
        return None


@dataclass
class BenchmarkRun:
    label: str
    n_tasks: int
    variant_count: int
    worker_concurrency_hint: int
    wall_time: float = 0.0
    results: list[TaskResult] = field(default_factory=list)

    # ── aggregate stats ──────────────────────────────────────────────────
    @property
    def successes(self):
        return [r for r in self.results if r.success]

    @property
    def failures(self):
        return [r for r in self.results if not r.success]

    @property
    def throughput(self) -> float:
        return len(self.successes) / self.wall_time if self.wall_time else 0.0

    def _stat(self, values: list[float]) -> dict:
        if not values:
            return {"min": None, "max": None, "mean": None, "p50": None, "p95": None}
        s = sorted(values)
        p95_idx = max(0, int(len(s) * 0.95) - 1)
        return {
            "min": round(min(s), 3),
            "max": round(max(s), 3),
            "mean": round(statistics.mean(s), 3),
            "p50": round(statistics.median(s), 3),
            "p95": round(s[p95_idx], 3),
        }

    @property
    def total_latency_stats(self):
        return self._stat([r.total_latency for r in self.successes if r.total_latency])

    @property
    def exec_time_stats(self):
        return self._stat(
            [r.execution_time for r in self.successes if r.execution_time]
        )

    @property
    def queue_latency_stats(self):
        return self._stat([r.queue_latency for r in self.successes if r.queue_latency])


# ───────────────────────────────────────────────────────────────────────────
# Core runner
# ───────────────────────────────────────────────────────────────────────────


def run_benchmark(
    n_tasks: int,
    variant_count: int,
    concurrency_hint: int,
    genome: str = "hg19",
    scores_table: str = "nc_spark.scores_hg19_normalized",
    gene_table: str = "nc_spark.nearest_gene_hg19",
    dry_run: bool = False,
    mock_sleep_ms: int = 200,
    timeout: float = 300.0,
    label: str = "",
) -> BenchmarkRun:
    """
    Dispatch `n_tasks` annotation jobs in parallel via Celery,
    each with `variant_count` variants (must be < SMALL_VARIANT_THRESHOLD=1000
    to exercise the small-query path).

    In dry_run mode, uses mock_annotate_task (no ClickHouse).
    """
    assert variant_count < 1000, (
        f"variant_count={variant_count} must be < 1000 to test the small-query path. "
        "Use a value like 100, 500, or 999."
    )

    run_label = label or f"n={n_tasks} v={variant_count} c={concurrency_hint}"
    run = BenchmarkRun(
        label=run_label,
        n_tasks=n_tasks,
        variant_count=variant_count,
        worker_concurrency_hint=concurrency_hint,
    )

    # ── Pre-generate session IDs and variants ────────────────────────────
    sessions = [_new_session_id() for _ in range(n_tasks)]
    _variants = generate_variants(
        variant_count
    )  # same set reused per task (reproducible)

    # ── Submit all tasks ─────────────────────────────────────────────────
    task_records: list[tuple[TaskResult, object]] = []  # (record, AsyncResult)

    wall_start = time.perf_counter()

    _print(f"\n▶  Submitting {n_tasks} tasks ({variant_count} variants each) …")
    for i, sid in enumerate(sessions):
        submit_ts = time.perf_counter()
        if dry_run:
            async_result = mock_annotate_task.apply_async(
                kwargs={
                    "session_id": sid,
                    "variant_count": variant_count,
                    "sleep_ms": mock_sleep_ms,
                },
                queue="celery",
            )
        else:
            # Real path: pre-upload variants, then fire the task.
            # We use a helper that mirrors production but skips the
            # job_status DB dependency — pass variants directly via kwargs
            # by calling a thin benchmark wrapper task (see below).
            async_result = benchmark_annotate_task.apply_async(
                kwargs={
                    "session_id": sid,
                    "variant_count": variant_count,
                    "variants": _variants,  # serialised as JSON list-of-lists
                    "scores_table": scores_table,
                    "gene_table": gene_table,
                },
                queue="celery",
            )

        record = TaskResult(
            session_id=sid,
            variant_count=variant_count,
            submit_ts=submit_ts,
        )
        task_records.append((record, async_result))

    _print(
        f"   All {n_tasks} tasks submitted in {time.perf_counter() - wall_start:.2f}s"
    )
    _print(f"   Waiting for results (timeout={timeout}s) …")

    # ── Poll for completion ───────────────────────────────────────────────
    pending = list(task_records)
    deadline = time.perf_counter() + timeout

    while pending and time.perf_counter() < deadline:
        still_pending = []
        for record, ar in pending:
            if ar.ready():
                record.end_ts = time.perf_counter()
                try:
                    result = ar.get(timeout=1)
                    record.success = True
                    if isinstance(result, dict):
                        record.rows_annotated = result.get("rows_annotated", 0)
                        # If task reports its own start time, use it
                        if "start_ts" in result:
                            record.start_ts = result["start_ts"]
                    # Fall back: use submit_ts as start proxy if no start reported
                    if record.start_ts is None:
                        record.start_ts = record.submit_ts
                except Exception as exc:
                    record.success = False
                    record.error = str(exc)
                    if record.start_ts is None:
                        record.start_ts = record.submit_ts
                run.results.append(record)
            else:
                still_pending.append((record, ar))
        pending = still_pending
        if pending:
            time.sleep(0.5)

    # ── Mark timed-out tasks ─────────────────────────────────────────────
    for record, ar in pending:
        record.success = False
        record.error = "timeout"
        record.end_ts = time.perf_counter()
        if record.start_ts is None:
            record.start_ts = record.submit_ts
        run.results.append(record)

    run.wall_time = time.perf_counter() - wall_start
    return run


# ───────────────────────────────────────────────────────────────────────────
# Benchmark wrapper Celery task (real ClickHouse path)
# Calls _build_small_query + remote execute directly — no job_status dep.
# ───────────────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="benchmark.annotate_task")
def benchmark_annotate_task(
    self,
    session_id: str,
    variant_count: int,
    variants: list,  # list of [chr, pos, ref, alt]
    scores_table: str,
    gene_table: str,
):
    """
    Minimal benchmark task that exercises the full small-query path:
      1. Insert variants into remote user_uploads
      2. Build & run _build_small_query
      3. Copy results back locally
      4. Cleanup remote

    Does NOT touch job_status DB so it can run standalone.
    """
    import time as _time

    start_ts = _time.perf_counter()

    # Lazy import inside task so worker only needs the annotation module
    from app.api.annotation import (
        _build_small_query,
        _copy_results_to_local,
        _cleanup_remote,
    )
    from app.session import get_remote_background_client, get_local_background_client

    variant_tuples = [tuple(v) for v in variants]

    remote_session = get_remote_background_client()
    local_session = get_local_background_client()

    try:
        # Upload variants to remote
        rows = [(session_id, *v) for v in variant_tuples]
        remote_session.insert(
            "nc_spark.user_uploads",
            rows,
            column_names=["session_id", "chr", "pos", "ref", "alt"],
        )

        # Build & run the small query
        query = _build_small_query(
            session_id=session_id,
            variants=variant_tuples,
            scores_table=scores_table,
            gene_table=gene_table,
        )
        remote_session.command(query)

        # Copy results locally
        rows_annotated = _copy_results_to_local(
            remote_session=remote_session,
            local_session=local_session,
            session_id=session_id,
        )

        elapsed = _time.perf_counter() - start_ts
        return {
            "session_id": session_id,
            "rows_annotated": rows_annotated,
            "elapsed": round(elapsed, 3),
            "start_ts": start_ts,
            "status": "complete",
        }

    except Exception as exc:
        raise
    finally:
        _cleanup_remote(remote_session, session_id)
        remote_session.close()
        local_session.close()


# ───────────────────────────────────────────────────────────────────────────
# Reporting
# ───────────────────────────────────────────────────────────────────────────


def _print(msg: str):
    if _RICH:
        console.print(msg)
    else:
        print(msg)


def print_run_summary(run: BenchmarkRun):
    tl = run.total_latency_stats
    et = run.exec_time_stats
    ql = run.queue_latency_stats

    lines = [
        f"\n{'─' * 60}",
        f"  Benchmark: {run.label}",
        f"{'─' * 60}",
        f"  Tasks:         {run.n_tasks}  ({len(run.successes)} ok, {len(run.failures)} failed)",
        f"  Variants/task: {run.variant_count}",
        f"  Wall time:     {run.wall_time:.2f}s",
        f"  Throughput:    {run.throughput:.2f} tasks/s",
        f"",
        f"  Total latency (submit→done)   min={tl['min']}s  p50={tl['p50']}s  p95={tl['p95']}s  max={tl['max']}s",
        f"  Execution time (start→done)   min={et['min']}s  p50={et['p50']}s  p95={et['p95']}s  max={et['max']}s",
        f"  Queue latency  (submit→start) min={ql['min']}s  p50={ql['p50']}s  p95={ql['p95']}s  max={ql['max']}s",
    ]
    if run.failures:
        lines.append(f"\n  ⚠  Failures:")
        for r in run.failures[:5]:  # show at most 5
            lines.append(f"     {r.session_id}: {r.error}")
    lines.append(f"{'─' * 60}\n")
    _print("\n".join(lines))


def save_results(runs: list[BenchmarkRun], out_dir: str = "."):
    """Write per-task CSV + summary JSON to out_dir."""
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    # Per-task CSV
    csv_path = os.path.join(out_dir, f"bench_tasks_{ts}.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run_label",
                "session_id",
                "variant_count",
                "submit_ts",
                "start_ts",
                "end_ts",
                "queue_latency",
                "execution_time",
                "total_latency",
                "rows_annotated",
                "success",
                "error",
            ],
        )
        writer.writeheader()
        for run in runs:
            for r in run.results:
                writer.writerow(
                    {
                        "run_label": run.label,
                        "session_id": r.session_id,
                        "variant_count": r.variant_count,
                        "submit_ts": round(r.submit_ts, 4),
                        "start_ts": round(r.start_ts, 4) if r.start_ts else "",
                        "end_ts": round(r.end_ts, 4) if r.end_ts else "",
                        "queue_latency": round(r.queue_latency, 4)
                        if r.queue_latency
                        else "",
                        "execution_time": round(r.execution_time, 4)
                        if r.execution_time
                        else "",
                        "total_latency": round(r.total_latency, 4)
                        if r.total_latency
                        else "",
                        "rows_annotated": r.rows_annotated,
                        "success": r.success,
                        "error": r.error or "",
                    }
                )
    _print(f"  ✓ Per-task CSV → {csv_path}")

    # Summary JSON
    summary_path = os.path.join(out_dir, f"bench_summary_{ts}.json")
    summary = []
    for run in runs:
        summary.append(
            {
                "label": run.label,
                "n_tasks": run.n_tasks,
                "variant_count": run.variant_count,
                "concurrency_hint": run.worker_concurrency_hint,
                "wall_time": round(run.wall_time, 3),
                "throughput": round(run.throughput, 3),
                "successes": len(run.successes),
                "failures": len(run.failures),
                "total_latency": run.total_latency_stats,
                "execution_time": run.exec_time_stats,
                "queue_latency": run.queue_latency_stats,
            }
        )
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    _print(f"  ✓ Summary JSON  → {summary_path}")

    return csv_path, summary_path


# ───────────────────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Benchmark parallel small-query annotation via Celery",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--tasks",
        type=int,
        default=32,
        help="Number of parallel annotation tasks to dispatch",
    )
    p.add_argument(
        "--variant-count",
        type=int,
        default=500,
        help="Variants per task (must be < 1000 for small-query path)",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Hint: expected Celery worker -c value (informational only)",
    )
    p.add_argument(
        "--genome", type=str, default="hg19", help="Genome build (hg19 or hg38)"
    )
    p.add_argument(
        "--scores-table", type=str, default="nc_spark.scores_hg19_normalized"
    )
    p.add_argument("--gene-table", type=str, default="nc_spark.nearest_gene_hg19")
    p.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="Max seconds to wait for all tasks to complete",
    )
    p.add_argument(
        "--out-dir",
        type=str,
        default="benchmark_results",
        help="Directory to write CSV/JSON results",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Use mock task (no ClickHouse) — benchmarks pure Celery/Redis overhead",
    )
    p.add_argument(
        "--mock-sleep-ms",
        type=int,
        default=200,
        help="Simulated task duration in ms when --dry-run is set",
    )
    p.add_argument(
        "--sweep",
        action="store_true",
        help=(
            "Run a concurrency sweep: automatically tests "
            "concurrency levels [1, 2, 4, 8, 16, 26, 32, 53] "
            "with --tasks tasks each. "
            "Note: you must restart Celery workers with matching -c for each level, "
            "or use --dry-run to sweep pure Redis overhead."
        ),
    )
    p.add_argument(
        "--sweep-levels",
        type=str,
        default="1,2,4,8,16,26,32,53",
        help="Comma-separated concurrency levels for --sweep",
    )
    p.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Number of warmup tasks to discard before timed run",
    )
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Sanity check
    if args.variant_count >= 1000:
        parser.error("--variant-count must be < 1000 to exercise the small-query path")

    all_runs: list[BenchmarkRun] = []

    if args.sweep:
        levels = [int(x) for x in args.sweep_levels.split(",")]
        _print(f"\n🔬  Concurrency sweep over levels: {levels}")
        _print("    (For real ClickHouse benchmarks, restart Celery workers")
        _print("     with matching -c before each level, or use --dry-run)\n")

        for lvl in levels:
            # Optional warmup
            if args.warmup > 0:
                _print(f"  Warming up {args.warmup} tasks at concurrency={lvl} …")
                run_benchmark(
                    n_tasks=args.warmup,
                    variant_count=args.variant_count,
                    concurrency_hint=lvl,
                    genome=args.genome,
                    scores_table=args.scores_table,
                    gene_table=args.gene_table,
                    dry_run=args.dry_run,
                    mock_sleep_ms=args.mock_sleep_ms,
                    timeout=args.timeout,
                    label=f"warmup c={lvl}",
                )

            run = run_benchmark(
                n_tasks=args.tasks,
                variant_count=args.variant_count,
                concurrency_hint=lvl,
                genome=args.genome,
                scores_table=args.scores_table,
                gene_table=args.gene_table,
                dry_run=args.dry_run,
                mock_sleep_ms=args.mock_sleep_ms,
                timeout=args.timeout,
                label=f"c={lvl} n={args.tasks} v={args.variant_count}",
            )
            print_run_summary(run)
            all_runs.append(run)

        # Print comparison table
        _print("\n📊  Sweep Summary")
        _print(
            f"  {'Concurrency':>12}  {'Tasks OK':>9}  {'Throughput':>12}  {'p50 lat':>9}  {'p95 lat':>9}  {'Wall time':>10}"
        )
        _print(f"  {'─' * 12}  {'─' * 9}  {'─' * 12}  {'─' * 9}  {'─' * 9}  {'─' * 10}")
        for r in all_runs:
            tl = r.total_latency_stats
            _print(
                f"  {r.worker_concurrency_hint:>12}  "
                f"{len(r.successes):>9}  "
                f"{r.throughput:>11.2f}/s  "
                f"{tl['p50']:>8}s  "
                f"{tl['p95']:>8}s  "
                f"{r.wall_time:>9.1f}s"
            )

    else:
        # Warmup
        if args.warmup > 0:
            _print(f"\nWarming up {args.warmup} tasks …")
            run_benchmark(
                n_tasks=args.warmup,
                variant_count=args.variant_count,
                concurrency_hint=args.concurrency,
                dry_run=args.dry_run,
                mock_sleep_ms=args.mock_sleep_ms,
                timeout=60.0,
                label="warmup",
                genome=args.genome,
                scores_table=args.scores_table,
                gene_table=args.gene_table,
            )

        run = run_benchmark(
            n_tasks=args.tasks,
            variant_count=args.variant_count,
            concurrency_hint=args.concurrency,
            genome=args.genome,
            scores_table=args.scores_table,
            gene_table=args.gene_table,
            dry_run=args.dry_run,
            mock_sleep_ms=args.mock_sleep_ms,
            timeout=args.timeout,
            label=f"c={args.concurrency} n={args.tasks} v={args.variant_count}",
        )
        print_run_summary(run)
        all_runs.append(run)

    save_results(all_runs, out_dir=args.out_dir)
    _print("\n✅  Benchmark complete.\n")


if __name__ == "__main__":
    main()
