import time
from celery import chord
from celery_app import celery_app
from datetime import datetime, timezone
from app.schema import USER_RESULTS_COLUMNS
from app.api.job_status import get_job_status, create_or_update_job_status
from app.session import get_remote_background_client, get_local_background_client

# ── Strategy constants ────────────────────────────────────────────────────────
# All jobs are split into 50K WHERE IN chunks dispatched as a Celery chord:
#   - chord header : N × annotate_chunk tasks (run in parallel)
#   - chord body   : finalise_annotations task (runs after ALL chunks succeed)
#
# The API (upload_variants2) calls dispatch_annotation_job() directly —
# no orchestrator task sits in between, so no worker is ever blocked waiting.

SMALL_QUERY_CHUNK_SIZE = 50_000  # WHERE IN chunk size — empirically optimal
COPY_PAGE_SIZE = 50_000  # paginated copy batch size

COLUMNS_SQL = ", ".join(USER_RESULTS_COLUMNS)


# ─────────────────────────────────────────────────────────────────────────────
# Query builder
# ─────────────────────────────────────────────────────────────────────────────


def _build_small_query(
    session_id: str,
    variants: list[tuple],
    scores_table: str,
    gene_table: str,
) -> str:
    """
    WHERE IN point-lookup strategy.
    Safe up to 50K variants within max_query_size=10MiB (~3.5MB of SQL text).
    """
    in_clause = ",\n                ".join(
        f"('{chr_}', {pos}, '{ref}', '{alt}')" for chr_, pos, ref, alt in variants
    )
    chr_pos_pairs = ",\n                ".join(
        f"('{chr_}', {pos})"
        for chr_, pos in dict.fromkeys((v[0], v[1]) for v in variants)
    )

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
SELECT
    '{session_id}' AS session_id,
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
    COALESCE(g.nearest_gene_plus, '')   AS nearest_gene_plus,
    g.plus_distance                     AS plus_distance,
    COALESCE(g.nearest_gene_minus, '') AS nearest_gene_minus,
    g.minus_distance                    AS minus_distance
FROM {scores_table} s
LEFT ANY JOIN (
    SELECT chr, pos, gene_if_overlapping, nearest_gene_plus,
           plus_distance, nearest_gene_minus, minus_distance
    FROM {gene_table}
    WHERE (chr, pos) IN (
        {chr_pos_pairs}
    )
) g ON s.chr = g.chr AND s.pos = g.pos
WHERE (s.chr, s.pos, s.ref, s.alt) IN (
    {in_clause}
)
SETTINGS
    join_algorithm = \'parallel_hash\',
    join_use_nulls = 1
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_uploaded_variants(session, session_id: str) -> list[tuple]:
    result = session.query(
        "SELECT chr, pos, ref, alt FROM nc_spark.user_uploads WHERE session_id = {sid:String}",
        parameters={"sid": session_id},
    )
    return list(result.result_rows)


def _copy_results_to_local(
    remote_session,
    local_session,
    session_id: str,
    target_session_id: str = None,
    clear_existing: bool = True,
) -> int:
    write_session_id = target_session_id or session_id
    COLUMNS_WITHOUT_SID = [c for c in USER_RESULTS_COLUMNS if c != "session_id"]
    columns_sql = ", ".join(COLUMNS_WITHOUT_SID)

    count_result = remote_session.query(
        "SELECT count() FROM nc_spark.user_results WHERE session_id = {sid:String}",
        parameters={"sid": session_id},
    )
    total_remote = count_result.result_rows[0][0] if count_result.result_rows else 0

    if total_remote == 0:
        print(f"[{session_id}] No rows on remote — annotation may not have completed.")
        return 0

    start_time = time.perf_counter()

    if clear_existing:
        try:
            local_session.command(
                "ALTER TABLE nc_spark.user_results DELETE WHERE session_id = {sid:String}",
                parameters={"sid": write_session_id},
            )
        except Exception as e:
            print(f"[{write_session_id}] Could not clear local rows: {e}")

    total_copied = 0
    offset = 0
    while offset < total_remote:
        page = remote_session.query(
            f"""
            SELECT {columns_sql}
            FROM nc_spark.user_results
            WHERE session_id = {{sid:String}}
            ORDER BY chr, pos, ref, alt
            LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}
            """,
            parameters={"sid": session_id, "limit": COPY_PAGE_SIZE, "offset": offset},
        )
        rows = page.result_rows
        if not rows:
            break
        local_session.insert(
            "nc_spark.user_results",
            [(write_session_id, *row) for row in rows],
            column_names=USER_RESULTS_COLUMNS,
        )
        total_copied += len(rows)
        offset += len(rows)

    if total_copied != total_remote:
        raise RuntimeError(
            f"[{session_id}] Integrity check failed: "
            f"expected {total_remote}, copied {total_copied}"
        )

    elapsed = time.perf_counter() - start_time
    print(
        f"[{session_id}] Copy complete: {total_copied} rows in {elapsed:.2f}s "
        f"({total_copied / elapsed:.0f} rows/s)"
    )
    return total_copied


def _cleanup_remote(session, session_id: str):
    """Best-effort remote cleanup — runs even on failure."""
    for table in ("nc_spark.user_uploads", "nc_spark.user_results"):
        try:
            session.command(
                f"ALTER TABLE {table} DELETE WHERE session_id = {{sid:String}}",
                parameters={"sid": session_id},
            )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Chunk task  — the unit of parallelism
# ─────────────────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="app.api.annotation.annotate_chunk")
def annotate_chunk(
    self,
    session_id: str,
    chunk_index: int,
    total_chunks: int,
    variants: list,  # list of [chr, pos, ref, alt]
    scores_table: str,
    gene_table: str,
) -> dict:
    """
    Annotates one 50K-variant chunk via WHERE IN and copies results locally.

    Uses chunk_session_id for remote isolation so concurrent chunks never
    collide on user_results. Local writes always use the original session_id
    (appending, never clearing sibling chunks).

    Returns dict consumed by finalise_annotations chord callback:
        {"chunk_index": int, "rows_annotated": int, "elapsed_s": float}
    """
    chunk_start = time.perf_counter()
    chunk_session_id = f"{session_id}_c{chunk_index}"
    variant_tuples = [tuple(v) for v in variants]

    remote_session = get_remote_background_client()
    local_session = get_local_background_client()

    try:
        query = _build_small_query(
            session_id=chunk_session_id,
            variants=variant_tuples,
            scores_table=scores_table,
            gene_table=gene_table,
        )
        remote_session.command(query)

        rows_annotated = _copy_results_to_local(
            remote_session=remote_session,
            local_session=local_session,
            session_id=chunk_session_id,
            target_session_id=session_id,
            clear_existing=False,  # append — never wipe sibling chunks
        )

        elapsed = round(time.perf_counter() - chunk_start, 3)
        print(
            f"[{session_id}] Chunk {chunk_index + 1}/{total_chunks}: "
            f"{rows_annotated} rows in {elapsed}s"
        )
        return {
            "chunk_index": chunk_index,
            "rows_annotated": rows_annotated,
            "elapsed_s": elapsed,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        print(f"[{session_id}] Chunk {chunk_index + 1}/{total_chunks} failed: {exc}")
        raise

    finally:
        _cleanup_remote(remote_session, chunk_session_id)
        remote_session.close()
        local_session.close()


# ─────────────────────────────────────────────────────────────────────────────
# Finaliser task  — chord callback, runs after ALL chunks succeed
# ─────────────────────────────────────────────────────────────────────────────


@celery_app.task(bind=True, name="app.api.annotation.finalise_annotations")
def finalise_annotations(self, chunk_results: list, session_id: str, job_start: float):
    """
    Chord callback — Celery calls this automatically once every annotate_chunk
    in the chord header has returned successfully.

    chunk_results: list of dicts returned by each annotate_chunk task, passed
                   in as the first positional argument by the chord machinery.
    """
    rows_annotated = sum(r["rows_annotated"] for r in chunk_results)
    completed_at = max(datetime.fromisoformat(r["finished_at"]) for r in chunk_results)
    job_start = datetime.fromisoformat(job_start)
    elapsed = round((completed_at - job_start).total_seconds(), 2)

    print(
        f"[{session_id}] All {len(chunk_results)} chunks complete: "
        f"{rows_annotated:,} rows in {elapsed}s "
        f"({rows_annotated / elapsed:,.0f} variants/s)"
    )
    create_or_update_job_status(
        session_id,
        status="complete",
        started_at=job_start,
        completed_at=completed_at,
        annotated_count=rows_annotated,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Error handler  — called by the API if the chord itself errors
# ─────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="app.api.annotation.handle_annotation_error")
def handle_annotation_error(request, exc, traceback, session_id: str):
    """
    Celery link_error callback — fires if any annotate_chunk task raises.
    Marks the session as failed so the client gets a definitive error state.
    """
    print(f"[{session_id}] Annotation failed: {exc}")
    create_or_update_job_status(
        session_id,
        status="error",
        error_message=str(exc),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public dispatch function  — called directly from upload_variants2 in annotate.py
# ─────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="app.api.annotation.dispatch_annotation_job")
def dispatch_annotation_job(session_id: str, variant_count: int):
    materialise_annotations(session_id, variant_count)


def materialise_annotations(session_id: str, variant_count: int):
    """
    Builds and dispatches the chord from the API layer (not from a Celery task).
    This avoids the 'Never call result.get() within a task' deadlock entirely —
    the chord is constructed in the FastAPI request thread, not in a worker.

    Returns the chord AsyncResult so the caller can store the task ID if needed.

    Flow:
        upload_variants2 (FastAPI)
            └─ materialise_annotations()          ← this function (plain Python)
                    └─ chord(
                         [annotate_chunk × N],    ← parallel header
                         finalise_annotations     ← callback after all succeed
                       ).apply_async()
    """
    job_start = datetime.now(timezone.utc).isoformat()
    local_session = get_local_background_client()

    try:
        create_or_update_job_status(session_id, status="processing")

        job_info = get_job_status(session_id)
        genome = job_info.genome if job_info else "hg19"
        scores_table = f"nc_spark.scores_{genome}_normalized"
        gene_table = f"nc_spark.nearest_gene_{genome}"

        variants = _fetch_uploaded_variants(local_session, session_id)
        if not variants:
            raise ValueError(f"No uploaded variants found for session {session_id}")

        chunks = [
            variants[i : i + SMALL_QUERY_CHUNK_SIZE]
            for i in range(0, len(variants), SMALL_QUERY_CHUNK_SIZE)
        ]
        total_chunks = len(chunks)

        print(
            f"[{session_id}] {len(variants):,} variants → "
            f"{total_chunks} chunk(s) × {SMALL_QUERY_CHUNK_SIZE:,}"
        )

        queue_name = "small" if variant_count <= 50_000 else "large"
        print(f"[{session_id}] Dispatching to queue: {queue_name}")

        header = [
            annotate_chunk.s(
                session_id,
                chunk_index,
                total_chunks,
                [list(v) for v in chunk],
                scores_table,
                gene_table,
            ).set(
                queue=queue_name,
                link_error=handle_annotation_error.s(session_id=session_id),
            )
            for chunk_index, chunk in enumerate(chunks)
        ]

        callback = finalise_annotations.s(
            session_id=session_id, job_start=job_start
        ).set(queue="finalise")

        result = chord(header)(callback)

        return result

    except Exception as e:
        print(f"[{session_id}] Dispatch failed: {e}")
        create_or_update_job_status(session_id, status="error", error_message=str(e))
        raise

    finally:
        local_session.close()
