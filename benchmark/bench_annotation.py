#!/usr/bin/env python3
"""
bench_annotation.py  —  Direct ClickHouse benchmark (no Celery, no FastAPI)

Tests ONLY the database layer:
  Step 1: insert 50k synthetic variants into user_uploads
  Step 2: run _build_large_query  (the fixed version)
  Step 3: read results back from user_results
  Step 4: clean up
  Step 5: print a timing breakdown

Run this FIRST before any FastAPI test so you know whether the
bottleneck is the DB or the application layer.

Usage:
  python bench_annotation.py --genome hg19 --variants 50000
  python bench_annotation.py --genome hg38 --variants 1000    # small sanity check
  python bench_annotation.py --genome hg19 --variants 200000  # stress test
"""

import argparse
import random
import time
import uuid
import clickhouse_connect

# ── Config ────────────────────────────────────────────────────────────────────
REMOTE_HOST = "172.15.1.61"  # change to your remote ClickHouse host
REMOTE_PORT = 80
REMOTE_USER = "default"
REMOTE_PASS = ""
REMOTE_CLICKHOUSE_PROXY = "clickhouse/"

# Realistic chromosome sizes (hg19 / hg38 are close enough for benchmarking)
CHR_SIZES = {
    "chr1": 249_000_000,
    "chr2": 243_000_000,
    "chr3": 198_000_000,
    "chr4": 191_000_000,
    "chr5": 181_000_000,
    "chr6": 171_000_000,
    "chr7": 159_000_000,
    "chr8": 146_000_000,
    "chr9": 141_000_000,
    "chr10": 135_000_000,
    "chr11": 135_000_000,
    "chr12": 133_000_000,
    "chr13": 115_000_000,
    "chr14": 107_000_000,
    "chr15": 102_000_000,
    "chr16": 90_000_000,
    "chr17": 83_000_000,
    "chr18": 80_000_000,
    "chr19": 59_000_000,
    "chr20": 63_000_000,
    "chr21": 48_000_000,
    "chr22": 51_000_000,
}
BASES = ["A", "T", "C", "G"]
CHROMS = list(CHR_SIZES.keys())
WEIGHTS = [CHR_SIZES[c] for c in CHROMS]  # sample proportional to chr size


def make_variants(n: int) -> list[tuple]:
    """Generate n realistic (chr, pos, ref, alt) tuples."""
    variants = set()
    while len(variants) < n:
        chr_ = random.choices(CHROMS, weights=WEIGHTS)[0]
        pos = random.randint(1, CHR_SIZES[chr_])
        ref = random.choice(BASES)
        alt = random.choice([b for b in BASES if b != ref])
        variants.add((chr_, pos, ref, alt))
    return list(variants)


def fmt(seconds: float) -> str:
    return f"{seconds:.3f}s"


def run_benchmark(genome: str, n_variants: int):
    session_id = f"bench_{uuid.uuid4().hex[:12]}"
    scores_table = f"nc_spark.scores_{genome}_normalized"
    gene_table = f"nc_spark.nearest_gene_{genome}"

    print(f"\n{'=' * 60}")
    print(f"  Benchmark: {n_variants:,} variants  |  genome: {genome}")
    print(f"  session_id: {session_id}")
    print(f"{'=' * 60}\n")

    client = clickhouse_connect.get_client(
        host=REMOTE_HOST,
        port=REMOTE_PORT,
        connect_timeout=30,
        username=REMOTE_USER,
        password=REMOTE_PASS,
        send_receive_timeout=600,
        proxy_path=REMOTE_CLICKHOUSE_PROXY,
    )

    client.command("SET max_threads = 320")
    client.command("SET max_streams_for_merge_tree_reading = 320")
    client.command("SET use_uncompressed_cache = 1")

    timings = {}

    # ── Step 1: Generate variants ─────────────────────────────────────────────
    t0 = time.perf_counter()
    variants = make_variants(n_variants)
    timings["generate"] = time.perf_counter() - t0
    print(
        f"[1] Generated {len(variants):,} unique variants          {fmt(timings['generate'])}"
    )

    # ── Step 2: Insert into user_uploads ─────────────────────────────────────
    t0 = time.perf_counter()
    rows = [(session_id, chr_, pos, ref, alt) for chr_, pos, ref, alt in variants]
    client.insert(
        "nc_spark.user_uploads",
        rows,
        column_names=["session_id", "chr", "pos", "ref", "alt"],
    )
    timings["upload"] = time.perf_counter() - t0
    print(
        f"[2] Inserted {len(rows):,} rows into user_uploads        {fmt(timings['upload'])}"
    )

    # ── Step 3: Run annotation query ─────────────────────────────────────────
    query = _build_large_query(
        session_id=session_id,
        scores_table=scores_table,
        gene_table=gene_table,
    )
    t0 = time.perf_counter()
    client.command(query, parameters={"sid": session_id})
    timings["annotate"] = time.perf_counter() - t0
    print(
        f"[3] Annotation query completed                           {fmt(timings['annotate'])}"
    )

    # ── Step 4: Count and read results ────────────────────────────────────────
    t0 = time.perf_counter()
    result = client.query(
        "SELECT count() FROM nc_spark.user_results WHERE session_id = {sid:String}",
        parameters={"sid": session_id},
    )
    annotated_count = result.result_rows[0][0]
    timings["count"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    client.query(
        f"SELECT * FROM nc_spark.user_results WHERE session_id = {{sid:String}}",
        parameters={"sid": session_id},
    )
    timings["fetch"] = time.perf_counter() - t0
    print(
        f"[4] Fetched {annotated_count:,} annotated rows from user_results  {fmt(timings['fetch'])}"
    )

    # ── Step 5: Cleanup ───────────────────────────────────────────────────────
    t0 = time.perf_counter()
    for table in ("nc_spark.user_uploads", "nc_spark.user_results"):
        client.command(
            f"DELETE FROM {table} WHERE session_id = {{sid:String}}",
            parameters={"sid": session_id},
        )
    timings["cleanup"] = time.perf_counter() - t0
    print(
        f"[5] Cleaned up remote tables                             {fmt(timings['cleanup'])}"
    )

    # ── Report ────────────────────────────────────────────────────────────────
    total = sum(v for k, v in timings.items() if k != "cleanup")
    hit_rate = annotated_count / n_variants * 100

    print(f"\n{'─' * 60}")
    print(f"  TIMING BREAKDOWN")
    print(f"{'─' * 60}")
    print(f"  Variant generation (client-side)  : {fmt(timings['generate'])}")
    print(f"  Insert to user_uploads            : {fmt(timings['upload'])}")
    print(
        f"  Annotation query                  : {fmt(timings['annotate'])}  ◄ main target"
    )
    print(f"  Read results from user_results    : {fmt(timings['fetch'])}")
    print(f"{'─' * 60}")
    print(f"  TOTAL (upload+annotate+fetch)     : {fmt(total)}")
    print(
        f"  Annotated / uploaded              : {annotated_count:,} / {n_variants:,}  ({hit_rate:.1f}%)"
    )
    print(f"  Target                            : < 30.000s")
    print(
        f"  Status                            : {'✅ PASS' if total < 30 else '❌ FAIL'}"
    )
    print(f"{'─' * 60}\n")

    client.close()
    return timings


def _build_large_query(session_id: str, scores_table: str, gene_table: str) -> str:
    """The fixed _build_large_query from annotation.py — paste your version here."""
    return f"""
INSERT INTO nc_spark.user_results
(session_id, chr, pos, ref, alt,
CADD, CSCAPE_NONCODING, DANN, FATHMM_MKL_NONCODING, FATHMM_XF_NONCODING,
GPN, GWRVIS, JARVIS, LINSIGHT, NCER, ORION, REMM,
GERP, PhyloP_100way, PhyloP_30way, MACIE_CONSERVED,
FUNSEQ2, FIRE, REGULOMEDB, MACIE_REGULATORY,
REPLISEQ_S2, REPLISEQ_G1B, REPLISEQ_S4, REPLISEQ_S1, REPLISEQ_G2, REPLISEQ_S3,
pathogenicity_mean, pathogenicity_median, pathogenicity_min, pathogenicity_max,
regulatory_mean, regulatory_median, regulatory_min, regulatory_max,
conservation_mean, conservation_median, conservation_min, conservation_max,
replication_timing_mean, replication_timing_median, replication_timing_min, replication_timing_max,
trinucleotide,
gene_if_overlapping,
nearest_gene_plus, plus_distance,
nearest_gene_minus, minus_distance)
WITH uploaded AS (
    SELECT chr, pos, ref, alt
    FROM   nc_spark.user_uploads
    WHERE  session_id = {{sid:String}}
)
SELECT
    {{sid:String}} AS session_id,
    s.chr, s.pos, s.ref, s.alt,
    s.CADD, s.CSCAPE_NONCODING, s.DANN, s.FATHMM_MKL_NONCODING, s.FATHMM_XF_NONCODING,
    s.GPN, s.GWRVIS, s.JARVIS, s.LINSIGHT, s.NCER, s.ORION, s.REMM,
    s.GERP, s.PhyloP_100way, s.PhyloP_30way, s.MACIE_CONSERVED,
    s.FUNSEQ2, s.FIRE, s.REGULOMEDB, s.MACIE_REGULATORY,
    s.REPLISEQ_S2, s.REPLISEQ_G1B, s.REPLISEQ_S4, s.REPLISEQ_S1, s.REPLISEQ_G2, s.REPLISEQ_S3,
    s.pathogenicity_mean, s.pathogenicity_median, s.pathogenicity_min, s.pathogenicity_max,
    s.regulatory_mean, s.regulatory_median, s.regulatory_min, s.regulatory_max,
    s.conservation_mean, s.conservation_median, s.conservation_min, s.conservation_max,
    s.replication_timing_mean, s.replication_timing_median, s.replication_timing_min, s.replication_timing_max,
    s.trinucleotide,
    COALESCE(g.gene_if_overlapping, '') AS gene_if_overlapping,
    COALESCE(g.nearest_gene_plus,   '') AS nearest_gene_plus,
    g.plus_distance                     AS plus_distance,
    COALESCE(g.nearest_gene_minus,  '') AS nearest_gene_minus,
    g.minus_distance                    AS minus_distance
FROM {scores_table} s
LEFT ANY JOIN (
    SELECT chr, pos, gene_if_overlapping, nearest_gene_plus,
           plus_distance, nearest_gene_minus, minus_distance
    FROM {gene_table}
    WHERE (chr, pos) IN (SELECT chr, pos FROM uploaded)
) g ON s.chr = g.chr AND s.pos = g.pos
PREWHERE (s.chr, s.pos) IN (SELECT chr, pos FROM uploaded)
WHERE   (s.chr, s.pos, s.ref, s.alt) IN (SELECT chr, pos, ref, alt FROM uploaded)
SETTINGS
    max_threads                        = 320,
    max_streams_for_merge_tree_reading = 320,
    join_algorithm                     = 'parallel_hash',
    max_bytes_in_join                  = 10737418240,
    join_use_nulls                     = 1,
    use_uncompressed_cache             = 1,
    optimize_move_to_prewhere          = 1
"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--genome", default="hg19", choices=["hg19", "hg38"])
    parser.add_argument("--variants", default=50_000, type=int)
    args = parser.parse_args()
    run_benchmark(args.genome, args.variants)
