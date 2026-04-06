import time
import random
import httpx
from pathlib import Path
from datetime import datetime, timezone, timedelta
import fireducks.pandas as pd

# Rich imports
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)
from rich.panel import Panel
from rich.table import Table

# --- Configuration ---
MASTER_FILE = "files/data_713856.tsv"
BASE_URL = "http://10.10.6.80/nvpp/api/v1"
UPLOAD_URL = f"{BASE_URL}/upload"
STATUS_URL = f"{BASE_URL}/status/"
REPETITIONS = 15
TEMP_DIR = Path("./benchmarking_files")
TEMP_DIR.mkdir(exist_ok=True)

IST = timezone(timedelta(hours=5, minutes=30))
MONITORING_LOG_FILE = "benchmark_monitoring_progress.tsv"

console = Console()


# ── Helpers ──────────────────────────────────────────────────────────────────


def now_ist() -> str:
    """Return current time as a formatted IST string."""
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")


def to_ist(dt) -> datetime | None:
    """
    Convert a datetime (or ISO string) to IST.
    Handles both full ISO timestamps and ClickHouse-style partial strings
    like '36:36.0' by anchoring them to the date of `created_at`.
    """
    if dt is None or (isinstance(dt, float) and pd.isna(dt)):
        return None
    if isinstance(dt, str):
        dt = dt.strip()
        if not dt:
            return None
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return None  # Unparseable partial strings are left as-is
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # assume UTC if no tz info
    return dt.astimezone(IST)


def fix_partial_clickhouse_ts(partial: str, date_str: str) -> datetime | None:
    """
    ClickHouse sometimes returns only 'MM:SS.f' truncated timestamps.
    Reconstruct a full datetime by borrowing the date + hour from `date_str`.

    Example:
        date_str  = "2026-03-24T17:06:36"  (created_at, UTC)
        partial   = "36:36.0"              (started_at or completed_at)
    Returns a UTC datetime.
    """
    if not partial or not isinstance(partial, str):
        return None
    try:
        base = datetime.fromisoformat(date_str.strip())
        # partial looks like "MM:SS.f"
        parts = partial.replace(",", ".").split(":")
        minutes = int(parts[0])
        seconds = float(parts[1]) if len(parts) > 1 else 0.0
        # Use the hour from `base`; if minutes rolled over, bump the hour
        # hour = base.hour
        if minutes < base.minute:  # e.g. base.minute=6, minutes=36 → same hr
            pass
        # Reconstruct
        dt = base.replace(
            minute=minutes,
            second=int(seconds),
            microsecond=int((seconds % 1) * 1_000_000),
        )
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def make_progress() -> Progress:
    """Shared rich Progress factory."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>5.1f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


# ── Phase 1 : Submit Jobs ─────────────────────────────────────────────────────


def submit_jobs(client: httpx.Client, master_df: pd.DataFrame) -> list[dict]:
    """
    Generate TSV subsets (with rich progress) then upload them in a
    *randomised* variant-count order, also tracked with rich.
    """
    # Randomise the order so jobs are NOT submitted as 100→200→300→…
    file_counts = [
        100,
        200,
        500,
        1000,
        1500,
        2000,
        5000,
        10000,
        20000,
        50000,
        75000,
        100000,
        150000,
        200000,
        300000,
        500000,
        1000000,
        1500000,
        2000000,
        5000000,
        10000000,
    ]
    # random.shuffle(file_counts)

    console.print(
        Panel(
            f"[bold green]Submission order:[/bold green] {file_counts}",
            title="🎲 Randomised Variant Counts",
            expand=False,
        )
    )

    pending_jobs: list[dict] = []
    total_uploads = len(file_counts) * REPETITIONS

    with make_progress() as progress:
        # ── sub-task 1 : file creation ────────────────────────────────────
        create_task = progress.add_task(
            "[yellow]Creating subset TSV files", total=len(file_counts)
        )

        file_paths: dict[int, Path] = {}
        for count in file_counts:
            if count > len(master_df):
                sample_df = master_df.sample(n=count, replace=True)
            else:
                sample_df = master_df.sample(n=count)
            fp = TEMP_DIR / f"data_{count}.tsv"
            sample_df.to_csv(fp, sep="\t", index=False)
            file_paths[count] = fp
            progress.advance(create_task)

        # ── sub-task 2 : uploads ──────────────────────────────────────────
        upload_task = progress.add_task("[green]Uploading files", total=total_uploads)

        for count in file_counts:
            fp = file_paths[count]
            batch_task = progress.add_task(
                f"  [dim]↳ {count} variants", total=REPETITIONS
            )

            for i in range(REPETITIONS):
                try:
                    with fp.open("rb") as fh:
                        resp = client.post(
                            UPLOAD_URL,
                            data={"genome": "hg19", "file_format": "tsv"},
                            files={"file": (fp.name, fh, "text/tab-separated-values")},
                            timeout=600.0,
                        )
                    if resp.status_code == 200:
                        pending_jobs.append(
                            {
                                "session_id": resp.json()["session_id"],
                                "requested_variants": count,
                                "run_index": i,
                                "submitted_at_ist": now_ist(),
                            }
                        )
                    else:
                        console.log(
                            f"[red]HTTP {resp.status_code} — {count} variants run {i}"
                        )
                except Exception as exc:
                    console.log(f"[red]Exception — {count} variants run {i}: {exc}")

                progress.advance(batch_task)
                progress.advance(upload_task)

            progress.update(batch_task, visible=False)

    console.print(
        f"[bold green]✅ Submitted {len(pending_jobs)} / {total_uploads} jobs.[/bold green]"
    )
    return pending_jobs


# ── Phase 2 : Monitor Jobs ────────────────────────────────────────────────────


def monitor_jobs(client: httpx.Client, pending_jobs: list[dict]) -> list[dict]:
    """
    Poll until all jobs finish.
    • Uses rich progress bar for live feedback.
    • Saves a checkpoint TSV after every poll cycle.
    """
    completed_results: list[dict] = []
    total_jobs = len(pending_jobs)
    poll_count = 0
    monitoring_log: list[dict] = []

    console.print(
        Panel(
            f"[cyan]Tracking [bold]{total_jobs}[/bold] jobs — checkpoint saved to "
            f"[italic]{MONITORING_LOG_FILE}[/italic] after every poll.",
            title="🕵️  Monitoring Phase",
            expand=False,
        )
    )

    with make_progress() as progress:
        monitor_task = progress.add_task("[cyan]Jobs completed", total=total_jobs)

        while pending_jobs:
            still_pending: list[dict] = []
            poll_count += 1

            for job in pending_jobs:
                try:
                    resp = client.get(f"{STATUS_URL}{job['session_id']}", timeout=30.0)
                    status_data = resp.json()

                    if status_data.get("status") == "complete":
                        completed_results.append({**job, **status_data})
                        progress.advance(monitor_task)
                    else:
                        still_pending.append(job)
                except Exception:
                    still_pending.append(job)

            pending_jobs = still_pending
            completed_count = total_jobs - len(pending_jobs)

            # ── Checkpoint ── save monitoring log after every poll cycle ──
            monitoring_log.append(
                {
                    "poll_number": poll_count,
                    "timestamp_ist": now_ist(),
                    "completed": completed_count,
                    "pending": len(pending_jobs),
                    "percent_complete": round((completed_count / total_jobs) * 100, 1),
                }
            )
            pd.DataFrame(monitoring_log).to_csv(
                MONITORING_LOG_FILE, sep="\t", index=False
            )

            if pending_jobs:
                time.sleep(300)

    console.print("[bold green]✅ All jobs completed.[/bold green]")
    return completed_results


# ── Main ──────────────────────────────────────────────────────────────────────

console.rule("[bold blue]📦 Benchmarking — 50k User Load Test")
console.print(f"[dim]Started at: {now_ist()}[/dim]\n")

# 1. Load master file
with make_progress() as progress:
    load_task = progress.add_task("[yellow]Loading master TSV…", total=None)
    master_df = pd.read_csv(MASTER_FILE, sep="\t")
    progress.update(load_task, total=1, completed=1)

console.print(
    f"[green]Master file loaded:[/green] {len(master_df):,} rows × {len(master_df.columns)} columns\n"
)

with httpx.Client() as client:
    # Phase 1 — Submit
    submitted_list = submit_jobs(client, master_df)
    pd.DataFrame(submitted_list).to_csv(
        "benchmark_submissions.tsv", sep="\t", index=False
    )
    console.print("[dim]Submissions saved → benchmark_submissions.tsv[/dim]\n")

    # Phase 2 — Monitor
    final_data = monitor_jobs(client, submitted_list)

# 3. Export full results with IST timestamps
if final_data:
    results_df = pd.DataFrame(final_data)

    # ── Parse & convert timestamps to IST ─────────────────────────────────
    # `created_at` arrives as a full ISO string (e.g. "2026-03-24T17:06:36")
    # `started_at` / `completed_at` may be truncated ClickHouse strings like "36:36.0"
    for row_idx, row in results_df.iterrows():
        created_raw = str(row.get("created_at", ""))

        for col in ("started_at", "completed_at"):
            raw = str(row.get(col, ""))
            # Detect truncated format: no 'T' and contains ':'
            if "T" not in raw and ":" in raw and created_raw:
                dt = fix_partial_clickhouse_ts(raw, created_raw)
            else:
                try:
                    dt = datetime.fromisoformat(raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                except Exception:
                    dt = None
            results_df.at[row_idx, col] = (
                dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST") if dt else None
            )

    # Convert `created_at` too
    results_df["created_at"] = results_df["created_at"].apply(
        lambda v: to_ist(v).strftime("%Y-%m-%d %H:%M:%S IST") if to_ist(v) else v
    )

    # `queued_at` — convert if present
    if "queued_at" in results_df.columns:
        results_df["queued_at"] = results_df["queued_at"].apply(
            lambda v: to_ist(v).strftime("%Y-%m-%d %H:%M:%S IST") if to_ist(v) else v
        )

    # ── Duration column (seconds) ─────────────────────────────────────────
    # Re-parse the now-formatted IST strings to compute duration
    def parse_ist_str(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S IST").replace(tzinfo=IST)
        except Exception:
            return None

    results_df["internal_duration_sec"] = results_df.apply(
        lambda r: (
            (
                parse_ist_str(r["completed_at"]) - parse_ist_str(r["started_at"])
            ).total_seconds()
            if parse_ist_str(r.get("completed_at"))
            and parse_ist_str(r.get("started_at"))
            else None
        ),
        axis=1,
    )

    results_df.to_csv("benchmark_full_analysis.tsv", sep="\t", index=False)

    # ── Rich summary table ─────────────────────────────────────────────────
    table = Table(title="📊 Benchmark Summary", show_lines=True)
    table.add_column("Variants", style="cyan", justify="right")
    table.add_column("Runs", justify="right")
    table.add_column("Avg Duration (s)", style="green", justify="right")

    for variants, grp in results_df.groupby("requested_variants"):
        avg_dur = grp["internal_duration_sec"].mean()
        table.add_row(
            str(variants),
            str(len(grp)),
            f"{avg_dur:.1f}" if pd.notna(avg_dur) else "N/A",
        )

    console.print(table)
    console.print(
        "\n[bold green]📊 Analysis exported → benchmark_full_analysis.tsv[/bold green]"
    )
    console.print(f"[bold green]📋 Monitoring log → {MONITORING_LOG_FILE}[/bold green]")
    console.print(f"[dim]Finished at: {now_ist()}[/dim]")
