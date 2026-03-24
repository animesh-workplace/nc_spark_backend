import time
from celery_app import celery_app
from app.schema import USER_RESULTS_COLUMNS
from app.api.job_status import get_job_status, create_or_update_job_status
from app.session import get_remote_background_client, get_local_background_client


SMALL_VARIANT_THRESHOLD = 1000
REPLICATION_CHUNK_SIZE = 50_000
LARGE_VARIANT_THRESHOLD = 200_000
COLUMNS_SQL = ", ".join(USER_RESULTS_COLUMNS)


def _process_chunked(
    session_id: str,
    variants: list[tuple],
    scores_table: str,
    gene_table: str,
    remote_session,
    local_session,
) -> int:
    """
    Handles variant_count > 2 lakh by splitting into 50k chunks.
    Each chunk is fully replicated → annotated → copied → cleaned before
    the next chunk begins. Results accumulate in local user_results.

    Returns total rows annotated across all chunks.
    """
    total_variants = len(variants)
    total_chunks = (
        total_variants + REPLICATION_CHUNK_SIZE - 1
    ) // REPLICATION_CHUNK_SIZE  # ceiling division
    total_annotated = 0

    for chunk_index in range(total_chunks):
        chunk_start = chunk_index * REPLICATION_CHUNK_SIZE
        chunk_end = min(chunk_start + REPLICATION_CHUNK_SIZE, total_variants)
        chunk = variants[chunk_start:chunk_end]

        # Use a stable sub-session ID so remote cleanup is chunk-scoped
        # Format: <session_id>_c<chunk_index> e.g. "abc123_c0", "abc123_c1"
        chunk_session_id = f"{session_id}_c{chunk_index}"

        chunk_rows_annotated = _process_single_chunk(
            session_id=session_id,
            chunk_session_id=chunk_session_id,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            chunk=chunk,
            scores_table=scores_table,
            gene_table=gene_table,
            remote_session=remote_session,
            local_session=local_session,
        )

        total_annotated += chunk_rows_annotated

        print(
            f"[{session_id}] Progress: {total_annotated}/{total_variants} variants annotated ({100 * total_annotated // total_variants}%)"
        )

    return total_annotated


def _process_single_chunk(
    session_id: str,
    chunk_session_id: str,
    chunk_index: int,
    total_chunks: int,
    chunk: list[tuple],
    scores_table: str,
    gene_table: str,
    remote_session,
    local_session,
) -> int:
    """
    Fully processes one 50k chunk:
      1. Replicate chunk rows to remote user_uploads (under chunk_session_id)
      2. Run JOIN annotation → remote user_results
      3. Copy remote user_results → local user_results (under original session_id)
      4. Clean up remote user_uploads + user_results for this chunk

    Uses chunk_session_id on remote so chunks don't collide with each other
    and cleanup is always chunk-scoped. Local results are always written
    under the original session_id so they appear unified to the client.

    Returns number of rows annotated in this chunk.
    """
    chunk_start_time = time.perf_counter()

    try:
        # ── 1. Replicate chunk to remote user_uploads ─────────────────────────
        rows_with_chunk_sid = [
            (chunk_session_id, chr_, pos, ref, alt) for chr_, pos, ref, alt in chunk
        ]

        remote_session.insert(
            "nc_spark.user_uploads",
            rows_with_chunk_sid,
            column_names=["session_id", "chr", "pos", "ref", "alt"],
        )

        # ── 2. Run JOIN annotation on remote using chunk_session_id ───────────
        # _build_large_query uses {sid:String} — substitute chunk_session_id
        # so the CTE reads only this chunk's uploads, not the whole session
        query = _build_large_query(
            session_id=chunk_session_id,  # remote writes results under chunk_session_id
            scores_table=scores_table,
            gene_table=gene_table,
        )

        remote_session.command(query, parameters={"sid": chunk_session_id})

        # ── 3. Copy remote results → local, rewriting session_id ──────────────
        # Results on remote carry chunk_session_id — we rewrite them to
        # original session_id on copy so the client sees one unified result set
        chunk_rows = _copy_results_to_local(
            remote_session=remote_session,
            local_session=local_session,
            session_id=chunk_session_id,  # read from remote under chunk ID
            target_session_id=session_id,  # write locally under original ID
            clear_existing=False,  # append — don't wipe earlier chunks
        )

        elapsed = time.perf_counter() - chunk_start_time
        print(
            f"[{session_id}] Chunk {chunk_index + 1}/{total_chunks} done: {chunk_rows} rows in {elapsed:.2f}s"
        )

        return chunk_rows

    finally:
        # ── 4. Always clean up remote data for this chunk ─────────────────────
        # Runs even if copy failed — prevents remote data accumulation
        for table in ("nc_spark.user_uploads", "nc_spark.user_results"):
            try:
                remote_session.command(
                    f"ALTER TABLE {table} DELETE WHERE session_id = {{sid:String}}",
                    parameters={"sid": chunk_session_id},
                )
            except Exception as e:
                print(
                    f"[{session_id}] Chunk {chunk_index + 1} remote cleanup failed for {table}: {e}"
                )


def _build_small_query(
    session_id: str,
    variants: list[tuple],
    scores_table: str,
    gene_table: str,
) -> str:
    """
    WHERE IN strategy for < 1000 variants.
    Builds an explicit tuple list so ClickHouse uses primary key point lookups
    instead of a full JOIN shuffle.
    """
    # Build (chr, pos, ref, alt) IN ((...), (...), ...) literal
    in_clause = ",\n                ".join(
        f"('{chr_}', {pos}, '{ref}', '{alt}')" for chr_, pos, ref, alt in variants
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
            regulatory_mean,    regulatory_median,    regulatory_min,    regulatory_max,
            conservation_mean,  conservation_median,  conservation_min,  conservation_max,
            replication_timing_mean, replication_timing_median, replication_timing_min, replication_timing_max,
            trinucleotide,
            gene_if_overlapping,
            nearest_gene_plus,  plus_distance,
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
            s.regulatory_mean,    s.regulatory_median,    s.regulatory_min,    s.regulatory_max,
            s.conservation_mean,  s.conservation_median,  s.conservation_min,  s.conservation_max,
            s.replication_timing_mean, s.replication_timing_median, s.replication_timing_min, s.replication_timing_max,
            s.trinucleotide,
            COALESCE(g.gene_if_overlapping, '') AS gene_if_overlapping,
            COALESCE(g.nearest_gene_plus,   '') AS nearest_gene_plus,
            g.plus_distance                     AS plus_distance,
            COALESCE(g.nearest_gene_minus,  '') AS nearest_gene_minus,
            g.minus_distance                    AS minus_distance
        FROM {scores_table} s
        LEFT JOIN {gene_table} g
            ON s.chr = g.chr AND s.pos = g.pos
        WHERE (s.chr, s.pos, s.ref, s.alt) IN (
                {in_clause}
        )
        SETTINGS
            optimize_move_to_prewhere = 1;
    """


def _build_large_query(
    session_id: str,
    scores_table: str,
    gene_table: str,
) -> str:
    """
    JOIN strategy for ≥ 1000 variants.
    Drives lookup from user_uploads CTE into scores table using primary key.
    Uses parameterised {sid:String} — pass parameters={"sid": session_id} on execution.
    """
    return f"""
        INSERT INTO nc_spark.user_results
            (session_id, chr, pos, ref, alt,
            CADD, CSCAPE_NONCODING, DANN, FATHMM_MKL_NONCODING, FATHMM_XF_NONCODING,
            GPN, GWRVIS, JARVIS, LINSIGHT, NCER, ORION, REMM,
            GERP, PhyloP_100way, PhyloP_30way, MACIE_CONSERVED,
            FUNSEQ2, FIRE, REGULOMEDB, MACIE_REGULATORY,
            REPLISEQ_S2, REPLISEQ_G1B, REPLISEQ_S4, REPLISEQ_S1, REPLISEQ_G2, REPLISEQ_S3,
            pathogenicity_mean, pathogenicity_median, pathogenicity_min, pathogenicity_max,
            regulatory_mean,    regulatory_median,    regulatory_min,    regulatory_max,
            conservation_mean,  conservation_median,  conservation_min,  conservation_max,
            replication_timing_mean, replication_timing_median, replication_timing_min, replication_timing_max,
            trinucleotide,
            gene_if_overlapping,
            nearest_gene_plus,  plus_distance,
            nearest_gene_minus, minus_distance)
        WITH uploaded AS (
            SELECT chr, pos, ref, alt
            FROM nc_spark.user_uploads
            WHERE session_id = {{sid:String}}
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
            s.regulatory_mean,    s.regulatory_median,    s.regulatory_min,    s.regulatory_max,
            s.conservation_mean,  s.conservation_median,  s.conservation_min,  s.conservation_max,
            s.replication_timing_mean, s.replication_timing_median, s.replication_timing_min, s.replication_timing_max,
            s.trinucleotide,
            COALESCE(g.gene_if_overlapping, '') AS gene_if_overlapping,
            COALESCE(g.nearest_gene_plus,   '') AS nearest_gene_plus,
            g.plus_distance                     AS plus_distance,
            COALESCE(g.nearest_gene_minus,  '') AS nearest_gene_minus,
            g.minus_distance                    AS minus_distance
        FROM {scores_table} s
        LEFT JOIN {gene_table} g
            ON s.chr = g.chr AND s.pos = g.pos
        PREWHERE s.chr IN (SELECT DISTINCT chr FROM uploaded)
            AND s.pos IN (SELECT DISTINCT pos FROM uploaded)
        WHERE (s.chr, s.pos, s.ref, s.alt) IN (SELECT chr, pos, ref, alt FROM uploaded)
        SETTINGS
            max_threads = 96,
            max_streams_for_merge_tree_reading = 96,
            join_algorithm = 'parallel_hash,grace_hash'
    """


def _fetch_uploaded_variants(session, session_id: str) -> list[tuple]:
    """
    Fetches the (chr, pos, ref, alt) tuples from user_uploads for the WHERE IN path.
    Only called when variant_count < SMALL_VARIANT_THRESHOLD.
    """
    result = session.query(
        "SELECT chr, pos, ref, alt FROM nc_spark.user_uploads WHERE session_id = {sid:String}",
        parameters={"sid": session_id},
    )
    return list(result.result_rows)


def _replicate_uploads_to_remote(
    local_session,
    remote_session,
    session_id: str,
    variants: list[tuple],
):
    """
    Copies (session_id, chr, pos, ref, alt) rows from local to remote
    in chunks so the remote JOIN query can find them.
    Skips replication if rows already exist on remote (idempotent re-run guard).
    """
    # Idempotency check — if remote already has rows, skip (aging re-queue guard)
    check = remote_session.query(
        "SELECT count() FROM nc_spark.user_uploads WHERE session_id = {sid:String}",
        parameters={"sid": session_id},
    )
    existing_count = check.result_rows[0][0] if check.result_rows else 0
    if existing_count >= len(variants):
        return

    # Insert in chunks to avoid sending one massive payload
    rows_with_sid = [
        (session_id, chr_, pos, ref, alt) for chr_, pos, ref, alt in variants
    ]

    for offset in range(0, len(rows_with_sid), REPLICATION_CHUNK_SIZE):
        chunk = rows_with_sid[offset : offset + REPLICATION_CHUNK_SIZE]
        remote_session.insert(
            "nc_spark.user_uploads",
            chunk,
            column_names=["session_id", "chr", "pos", "ref", "alt"],
        )
        print(
            f"[{session_id}] Replicated chunk {offset}–{offset + len(chunk)} of {len(rows_with_sid)} to remote"
        )


def _copy_results_to_local(
    remote_session,
    local_session,
    session_id: str,
    target_session_id: str = None,  # if set, rewrites session_id on insert
    clear_existing: bool = True,  # set False when appending chunks
) -> int:
    """
    Streams annotated rows from remote nc_spark.user_results to local.

    Args:
        remote_session     : background client for remote ClickHouse
        local_session      : background client for local ClickHouse
        session_id         : session whose rows to read from remote
        target_session_id  : if provided, rows are written locally under
                             this ID instead of session_id. Used when
                             chunk sub-IDs (abc_c0, abc_c1) need to be
                             unified under the original session_id.
        clear_existing     : if True, deletes any existing local rows for
                             target_session_id before copying (idempotent
                             full-session copy). Set False for chunk appends
                             so earlier chunks are not wiped.

    Returns:
        int: total rows copied in this call
    """
    write_session_id = target_session_id or session_id

    COLUMNS_WITHOUT_SID = [c for c in USER_RESULTS_COLUMNS if c != "session_id"]
    columns_sql = ", ".join(COLUMNS_WITHOUT_SID)

    # ── Step 1: Count rows on remote ──────────────────────────────────────────
    count_result = remote_session.query(
        "SELECT count() FROM nc_spark.user_results WHERE session_id = {sid:String}",
        parameters={"sid": session_id},
    )
    total_remote = count_result.result_rows[0][0] if count_result.result_rows else 0

    if total_remote == 0:
        print(
            f"[{session_id}] No rows found in remote user_results — "
            "annotation may not have completed."
        )
        return 0

    start_time = time.perf_counter()

    # ── Step 2: Optionally clear existing local rows ──────────────────────────
    if clear_existing:
        try:
            local_session.command(
                "ALTER TABLE nc_spark.user_results DELETE WHERE session_id = {sid:String}",
                parameters={"sid": write_session_id},
            )
        except Exception as e:
            print(f"[{write_session_id}] Could not clear local rows: {e}")

    # ── Step 3: Paginated fetch → insert ─────────────────────────────────────
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
            parameters={
                "sid": session_id,
                "limit": REPLICATION_CHUNK_SIZE,
                "offset": offset,
            },
        )

        rows = page.result_rows
        if not rows:
            break

        # Prepend the write session_id (may differ from source session_id)
        rows_to_insert = [(write_session_id, *row) for row in rows]

        local_session.insert(
            "nc_spark.user_results",
            rows_to_insert,
            column_names=USER_RESULTS_COLUMNS,
        )

        total_copied += len(rows)
        offset += len(rows)

        print(f"[{session_id}] Copied {total_copied}/{total_remote} rows")

    # ── Step 4: Integrity check ───────────────────────────────────────────────
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
    """Best-effort cleanup of partial data on remote after task failure."""
    for table in ("nc_spark.user_uploads", "nc_spark.user_results"):
        try:
            session.command(
                f"ALTER TABLE {table} DELETE WHERE session_id = {{sid:String}}",
                parameters={"sid": session_id},
            )
        except Exception:
            pass


# Main Task
@celery_app.task(bind=True)
def materialise_annotations(self, session_id: str, variant_count: int):
    start_time = time.perf_counter()

    create_or_update_job_status(session_id, status="processing")

    remote_session = get_remote_background_client()
    local_session = get_local_background_client()
    try:
        job_info = get_job_status(session_id)
        genome = job_info.genome if job_info else "hg19"
        scores_table = f"nc_spark.scores_{genome}_normalized"
        gene_table = f"nc_spark.nearest_gene_{genome}"
        variants = _fetch_uploaded_variants(local_session, session_id)
        if not variants:
            raise ValueError(f"No uploaded variants found for session {session_id}")

        # Strategy switch
        if variant_count < SMALL_VARIANT_THRESHOLD:
            # ── Path A: WHERE IN (<1000)
            query = _build_small_query(
                variants=variants,
                session_id=session_id,
                gene_table=gene_table,
                scores_table=scores_table,
            )
            # Small query is fully inlined — no parameters needed
            remote_session.command(query)
            rows_annotated = _copy_results_to_local(
                remote_session=remote_session,
                local_session=local_session,
                session_id=session_id,
            )

        elif variant_count <= LARGE_VARIANT_THRESHOLD:
            # ── Path B: single JOIN (1000–200k)
            _replicate_uploads_to_remote(
                variants=variants,
                session_id=session_id,
                local_session=local_session,
                remote_session=remote_session,
            )

            query = _build_large_query(
                session_id=session_id,
                gene_table=gene_table,
                scores_table=scores_table,
            )
            # Large query uses parameterised {sid:String}
            remote_session.command(query, parameters={"sid": session_id})
            rows_annotated = _copy_results_to_local(
                remote_session=remote_session,
                local_session=local_session,
                session_id=session_id,
            )

        else:
            # ── Path C: chunked JOIN (>200k)
            rows_annotated = _process_chunked(
                variants=variants,
                session_id=session_id,
                gene_table=gene_table,
                scores_table=scores_table,
                local_session=local_session,
                remote_session=remote_session,
            )

        elapsed = time.perf_counter() - start_time
        print(f"[{session_id}] Completed in {elapsed:.2f}s")
        print(f"[{session_id}] Copy complete: {rows_annotated} rows annotated")

        create_or_update_job_status(
            session_id,
            status="complete",
            annotated_count=rows_annotated,
        )

    except Exception as e:
        print(f"[{session_id}] Materialisation failed: {e}")
        create_or_update_job_status(
            session_id,
            status="error",
            error_message=str(e),
        )
        _cleanup_remote(remote_session, session_id)

    finally:
        remote_session.close()
        local_session.close()
