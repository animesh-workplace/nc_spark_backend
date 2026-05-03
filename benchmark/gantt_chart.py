"""
gantt_chart.py — NC-SPARK Queue Benchmark · Gantt Chart Generator
==================================================================
Reads  : benchmark_full_analysis.tsv   (produced by benchmark.py)
Outputs: gantt_chart.png               (publication-quality Gantt)
         gantt_summary.tsv             (per-job stats)

Usage:
    python gantt_chart.py                          # default input file
    python gantt_chart.py --input my_results.tsv   # custom input
    python gantt_chart.py --top 60                 # show only first 60 jobs
    python gantt_chart.py --sort variants           # sort rows by variant count
    python gantt_chart.py --sort start              # sort rows by start time (default)

Dependencies:
    pip install matplotlib pandas rich
    (fireducks.pandas compatible — falls back to standard pandas automatically)
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# ── pandas shim: prefer fireducks if available ────────────────────────────────
try:
    import fireducks.pandas as pd
    _PD_BACKEND = "fireducks"
except ImportError:
    import pandas as pd
    _PD_BACKEND = "pandas"

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter
import numpy as np

IST = timezone(timedelta(hours=5, minutes=30))
console = Console()

# ── Colour palette (dark theme matching NC-SPARK branding) ───────────────────
BG_COLOR     = "#0d1117"
SURFACE_COL  = "#161b22"
BORDER_COL   = "#30363d"
TEXT_COL     = "#e6edf3"
MUTED_COL    = "#8b949e"
FAINT_COL    = "#484f58"

# Wait-time hatch colour
WAIT_COL     = "#30363d"

# Per–variant-count colour scale (log scale, 21 distinct sizes → rainbow → fixed palette)
VARIANT_PALETTE = [
    "#3fb950",  # 100        → green (fastest)
    "#4dc87a",
    "#56d68b",
    "#8ee26c",
    "#c0df45",
    "#d4c400",
    "#e6a817",
    "#f08c1f",
    "#f07230",
    "#e85840",
    "#e03e50",
    "#d42b68",
    "#bc2880",
    "#9e269e",
    "#7a26b8",
    "#5828cc",
    "#3a32dc",
    "#2244e8",
    "#1862f0",
    "#1480f4",
    "#10a0f8",  # 10 000 000 → bright blue (slowest)
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_ist_str(s: str | float) -> datetime | None:
    """Parse an IST timestamp string back to a timezone-aware datetime."""
    if not s or (isinstance(s, float) and np.isnan(s)):
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S IST", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s.replace(" IST", ""), fmt.replace(" IST", ""))
            dt = dt.replace(tzinfo=IST); return dt.replace(tzinfo=None)
        except ValueError:
            continue
    return None


def seconds_to_label(s: float) -> str:
    if s < 0:
        return "0s"
    if s < 60:
        return f"{s:.1f}s"
    if s < 3600:
        return f"{s/60:.1f}m"
    return f"{s/3600:.1f}h"


def load_and_prepare(path: Path) -> pd.DataFrame:
    """Load benchmark TSV and compute timing columns."""
    df = pd.read_csv(path, sep="\t")

    required = {"requested_variants", "started_at", "completed_at"}
    missing = required - set(df.columns)
    if missing:
        console.print(f"[red]❌ Missing columns in TSV: {missing}[/red]")
        sys.exit(1)

    # Parse timestamps
    df["_started"]   = df["started_at"].apply(parse_ist_str)
    df["_completed"] = df["completed_at"].apply(parse_ist_str)

    # Also parse created_at / queued_at for wait time if available
    if "created_at" in df.columns:
        df["_created"] = df["created_at"].apply(parse_ist_str)
    elif "queued_at" in df.columns:
        df["_created"] = df["queued_at"].apply(parse_ist_str)
    else:
        df["_created"] = df["_started"]   # no wait info → assume 0 wait

    # Drop rows with unparseable timestamps
    before = len(df)
    df = df.dropna(subset=["_started", "_completed"])
    dropped = before - len(df)
    if dropped:
        console.print(f"[yellow]⚠ Dropped {dropped} rows with unparseable timestamps.[/yellow]")

    # Anchor everything to t=0 at the earliest event
    t0 = df["_started"].min()
    if df["_created"].notna().any():
        t0 = min(t0, df["_created"].dropna().min())

    df["start_sec"]    = df["_started"].apply(lambda d: (d - t0).total_seconds() if d else None)
    df["end_sec"]      = df["_completed"].apply(lambda d: (d - t0).total_seconds() if d else None)
    df["exec_sec"]     = df["end_sec"] - df["start_sec"]
    df["created_sec"]  = df["_created"].apply(lambda d: (d - t0).total_seconds() if d else df["start_sec"])
    df["wait_sec"]     = (df["start_sec"] - df["created_sec"]).clip(lower=0)

    # Duration from pre-computed column if present
    if "internal_duration_sec" in df.columns:
        df["exec_sec"] = pd.to_numeric(df["internal_duration_sec"], errors="coerce").fillna(df["exec_sec"])

    # Run label  e.g. "100 variants — run 3"
    if "run_index" in df.columns:
        df["label"] = df.apply(
            lambda r: f"{int(r.requested_variants):>10,} vars · run {int(r.run_index)+1}", axis=1
        )
    else:
        df["label"] = df["requested_variants"].apply(lambda v: f"{int(v):>10,} vars")

    return df.dropna(subset=["start_sec", "end_sec", "exec_sec"])


def assign_colors(df: pd.DataFrame) -> pd.DataFrame:
    """Map each unique variant count to a colour from the palette."""
    counts = sorted(df["requested_variants"].unique())
    palette = VARIANT_PALETTE
    step = max(1, len(palette) // max(len(counts), 1))
    color_map = {c: palette[min(i * step, len(palette) - 1)] for i, c in enumerate(counts)}
    df["bar_color"] = df["requested_variants"].map(color_map)
    return df, color_map


def plot_gantt(
    df: pd.DataFrame,
    color_map: dict,
    output_path: Path,
    top: int,
    sort_by: str,
) -> None:
    """Render the Gantt chart."""

    # ── Sort & slice ──────────────────────────────────────────────────────────
    if sort_by == "variants":
        df = df.sort_values(["requested_variants", "start_sec"])
    elif sort_by == "wait":
        df = df.sort_values("wait_sec", ascending=False)
    else:  # start (default)
        df = df.sort_values("start_sec")

    df = df.head(top).reset_index(drop=True)
    n_rows = len(df)

    total_span = df["end_sec"].max()

    # ── Figure setup ─────────────────────────────────────────────────────────
    row_h     = 0.55       # height per Gantt row (inches)
    fig_h     = max(8, n_rows * row_h + 4)
    fig_w     = 20

    fig, axes = plt.subplots(
        2, 1,
        figsize=(fig_w, fig_h),
        gridspec_kw={"height_ratios": [n_rows * row_h, 3.2], "hspace": 0.38},
        facecolor=BG_COLOR,
    )
    ax_gantt, ax_bar = axes

    # ── Dark background for both axes ────────────────────────────────────────
    for ax in axes:
        ax.set_facecolor(SURFACE_COL)
        ax.tick_params(colors=MUTED_COL, labelsize=9)
        for spine in ax.spines.values():
            spine.set_edgecolor(BORDER_COL)

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 1 — Gantt
    # ─────────────────────────────────────────────────────────────────────────
    bar_height = 0.65
    y_positions = np.arange(n_rows)

    for idx, row in df.iterrows():
        y = idx

        # Wait block (hatched)
        if row["wait_sec"] > 0.1:
            ax_gantt.barh(
                y, row["wait_sec"],
                left=row["created_sec"],
                height=bar_height,
                color=WAIT_COL,
                edgecolor=BORDER_COL,
                linewidth=0.4,
                hatch="//",
                zorder=2,
            )

        # Execution bar
        ax_gantt.barh(
            y, row["exec_sec"],
            left=row["start_sec"],
            height=bar_height,
            color=row["bar_color"],
            edgecolor="none",
            alpha=0.88,
            zorder=3,
        )

        # Duration label inside bar (if wide enough)
        label_x = row["start_sec"] + row["exec_sec"] / 2
        if row["exec_sec"] / total_span > 0.03:
            ax_gantt.text(
                label_x, y,
                seconds_to_label(row["exec_sec"]),
                ha="center", va="center",
                fontsize=7.5, color="#000000cc",
                fontweight="bold", zorder=4,
            )

    # Y-axis labels
    ax_gantt.set_yticks(y_positions)
    ax_gantt.set_yticklabels(df["label"], fontsize=8, color=MUTED_COL, fontfamily="monospace")
    ax_gantt.invert_yaxis()
    ax_gantt.set_xlim(0, total_span * 1.02)

    # X-axis — seconds formatter
    ax_gantt.xaxis.set_major_formatter(FuncFormatter(lambda v, _: seconds_to_label(v)))
    ax_gantt.set_xlabel("Wall-clock time", color=MUTED_COL, fontsize=10, labelpad=6)

    # Light vertical gridlines
    ax_gantt.xaxis.grid(True, color=BORDER_COL, linestyle="--", linewidth=0.5, alpha=0.5, zorder=1)
    ax_gantt.set_axisbelow(True)

    ax_gantt.set_title(
        "NC-SPARK Job Timeline  (bar = execution · hatch = queue wait)",
        color=TEXT_COL, fontsize=13, fontweight="bold", pad=10, loc="left",
    )

    # ── Legend ────────────────────────────────────────────────────────────────
    legend_patches = [
        mpatches.Patch(facecolor=col, label=f"{int(vc):,}", alpha=0.88)
        for vc, col in color_map.items()
        if vc in df["requested_variants"].values
    ]
    wait_patch = mpatches.Patch(
        facecolor=WAIT_COL, hatch="//", edgecolor=BORDER_COL, label="Queue wait"
    )
    legend_patches.insert(0, wait_patch)

    ax_gantt.legend(
        handles=legend_patches,
        title="Variants",
        title_fontsize=8,
        fontsize=7.5,
        loc="lower right",
        ncol=max(1, len(legend_patches) // 5),
        framealpha=0.15,
        edgecolor=BORDER_COL,
        labelcolor=MUTED_COL,
        facecolor=SURFACE_COL,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 2 — Average execution time per variant count (log-scale bar)
    # ─────────────────────────────────────────────────────────────────────────
    summary = (
        df.groupby("requested_variants")["exec_sec"]
        .agg(["mean", "std", "count"])
        .reset_index()
        .sort_values("requested_variants")
    )
    summary["std"] = summary["std"].fillna(0)

    x_pos  = np.arange(len(summary))
    colors = [color_map.get(v, "#8b949e") for v in summary["requested_variants"]]

    bars = ax_bar.bar(
        x_pos,
        summary["mean"],
        yerr=summary["std"],
        color=colors,
        alpha=0.88,
        error_kw={"ecolor": MUTED_COL, "capsize": 3, "linewidth": 1.2},
        zorder=3,
    )

    # Value labels on top of each bar
    for bar, (_, row2) in zip(bars, summary.iterrows()):
        ax_bar.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + row2["std"] * 1.1 + 0.02 * summary["mean"].max(),
            seconds_to_label(row2["mean"]),
            ha="center", va="bottom",
            fontsize=8, color=TEXT_COL, fontweight="bold",
        )

    ax_bar.set_yscale("log")
    ax_bar.set_xticks(x_pos)
    ax_bar.set_xticklabels(
        [f"{int(v):,}" for v in summary["requested_variants"]],
        rotation=45, ha="right", fontsize=8.5, color=MUTED_COL, fontfamily="monospace",
    )
    ax_bar.set_ylabel("Avg exec time (log scale)", color=MUTED_COL, fontsize=10)
    ax_bar.yaxis.set_major_formatter(FuncFormatter(lambda v, _: seconds_to_label(v)))
    ax_bar.xaxis.grid(False)
    ax_bar.yaxis.grid(True, color=BORDER_COL, linestyle="--", linewidth=0.5, alpha=0.5, zorder=1)
    ax_bar.set_axisbelow(True)
    ax_bar.set_title(
        "Average Execution Time per Variant Count  (± 1 std dev, log scale)",
        color=TEXT_COL, fontsize=11, fontweight="bold", pad=8, loc="left",
    )

    # ── Footer annotation ────────────────────────────────────────────────────
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    fig.text(
        0.99, 0.005,
        f"NC-SPARK Benchmark  ·  generated {timestamp}  ·  pandas backend: {_PD_BACKEND}",
        ha="right", va="bottom",
        fontsize=8, color=FAINT_COL,
    )

    plt.savefig(output_path, dpi=160, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)


def print_summary_table(df: pd.DataFrame) -> None:
    """Rich summary table to terminal."""
    summary = (
        df.groupby("requested_variants")["exec_sec"]
        .agg(["mean", "min", "max", "std", "count"])
        .reset_index()
        .sort_values("requested_variants")
    )

    table = Table(title="📊 Benchmark Summary — Execution Times", show_lines=True)
    table.add_column("Variants",      style="cyan",   justify="right")
    table.add_column("Runs",          style="white",  justify="right")
    table.add_column("Mean",          style="green",  justify="right")
    table.add_column("Min",           style="yellow", justify="right")
    table.add_column("Max",           style="red",    justify="right")
    table.add_column("Std Dev",       style="magenta",justify="right")

    for _, row in summary.iterrows():
        table.add_row(
            f"{int(row.requested_variants):,}",
            str(int(row["count"])),
            seconds_to_label(row["mean"]),
            seconds_to_label(row["min"]),
            seconds_to_label(row["max"]),
            seconds_to_label(row["std"]) if row["std"] > 0 else "—",
        )
    console.print(table)


def export_summary_tsv(df: pd.DataFrame, path: Path) -> None:
    summary = (
        df.groupby("requested_variants")["exec_sec"]
        .agg(mean="mean", median="median", min="min", max="max", std="std", count="count")
        .reset_index()
        .sort_values("requested_variants")
    )
    summary["mean_label"] = summary["mean"].apply(seconds_to_label)
    summary.to_csv(path, sep="\t", index=False)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate Gantt chart from NC-SPARK benchmark TSV output."
    )
    p.add_argument(
        "--input", "-i",
        default="benchmark_full_analysis.tsv",
        help="Path to benchmark_full_analysis.tsv (default: benchmark_full_analysis.tsv)",
    )
    p.add_argument(
        "--output", "-o",
        default="gantt_chart.png",
        help="Output PNG path (default: gantt_chart.png)",
    )
    p.add_argument(
        "--top", "-n",
        type=int, default=80,
        help="Max number of rows to show in Gantt (default: 80)",
    )
    p.add_argument(
        "--sort", "-s",
        choices=["start", "variants", "wait"],
        default="start",
        help="Sort rows by: start time (default), variants count, or wait time",
    )
    return p.parse_args()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    input_path  = Path(args.input)
    output_path = Path(args.output)
    summary_path = output_path.with_name(output_path.stem + "_summary.tsv")

    console.rule("[bold blue]⚡ NC-SPARK Gantt Chart Generator")

    if not input_path.exists():
        console.print(
            Panel(
                f"[red]File not found:[/red] [bold]{input_path}[/bold]\n\n"
                "Run [cyan]benchmark.py[/cyan] first to produce the results TSV, "
                "or point --input at an existing file.",
                title="❌ Input Error",
                expand=False,
            )
        )
        sys.exit(1)

    console.print(f"[dim]Loading [bold]{input_path}[/bold] (backend: {_PD_BACKEND}) …[/dim]")
    df = load_and_prepare(input_path)
    console.print(f"[green]✓ Loaded {len(df):,} completed jobs.[/green]")

    df, color_map = assign_colors(df)

    console.print(f"[dim]Rendering Gantt chart (top={args.top}, sort={args.sort}) …[/dim]")
    plot_gantt(df, color_map, output_path, top=args.top, sort_by=args.sort)
    console.print(f"[bold green]✅ Chart saved → {output_path}[/bold green]")

    export_summary_tsv(df, summary_path)
    console.print(f"[green]✅ Summary TSV saved → {summary_path}[/green]\n")

    print_summary_table(df)

    console.rule("[dim]Done[/dim]")


if __name__ == "__main__":
    main()
