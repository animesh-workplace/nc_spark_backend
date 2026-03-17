import time
import uuid
from fastapi import HTTPException
from fastapi import BackgroundTasks
from app.models import SessionStatus
from datetime import datetime, timezone, timedelta
from app.session import get_background_client, SessionLocal


# utils.py
# def set_session_status(
#     session, session_id: str, status: str, variant_count: int = 0, error_msg: str = None
# ):
#     session.command(
#         """
#         INSERT INTO nc_spark.session_status
#             (session_id, status, variant_count, error_msg, updated_at)
#         VALUES
#             ({sid:String}, {status:String}, {count:UInt32},
#              {err:Nullable(String)}, now())
#         """,
#         parameters={
#             "sid": session_id,
#             "status": status,
#             "count": variant_count,
#             "err": error_msg,
#         },
#     )


_STATUS_TIMESTAMP = {
    "queued": "queued_at",
    "processing": "started_at",
    "complete": "completed_at",
    "error": "completed_at",
}


def get_session_status(session_id: str) -> SessionStatus | None:
    db = SessionLocal()
    try:
        return db.get(SessionStatus, session_id)
    finally:
        db.close()


def set_session_status(
    session_id: str,
    *,
    # create-only fields
    genome: str = None,
    ttl_hours: int = 24,
    vcf_hash: str = None,
    queue_name: str = None,
    variant_count: int = None,
    # update fields
    status: str = None,
    from_cache: bool = False,
    error_message: str = None,
    celery_task_id: str = None,
    annotated_count: int = None,
) -> SessionStatus:
    """
    Creates or updates a session_status record.
    Manages its own DB connection internally.
    """
    db = SessionLocal()
    try:
        session = db.get(SessionStatus, session_id)

        if session is None:
            # ── CREATE ────────────────────────────────────────────
            session = SessionStatus(
                vcf_hash=vcf_hash,
                session_id=session_id,
                from_cache=from_cache,
                genome=genome or "hg19",
                status=status or "pending",
                variant_count=variant_count or 0,
                queue_name=queue_name or "large",
                expires_at=datetime.now(timezone.utc) + timedelta(hours=ttl_hours),
            )
            db.add(session)

        else:
            # ── UPDATE ────────────────────────────────────────────
            if status is not None:
                session.status = status
                ts_field = _STATUS_TIMESTAMP.get(status)
                if ts_field:
                    setattr(session, ts_field, datetime.now(timezone.utc))

            if annotated_count is not None:
                session.annotated_count = annotated_count
            if error_message is not None:
                session.error_message = error_message
            if celery_task_id is not None:
                session.celery_task_id = celery_task_id
            if from_cache:
                session.from_cache = True

        db.commit()
        db.refresh(session)
        return session

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# tasks.py
def materialise_annotations(session_id: str, variant_count: int):
    """
    Runs after the HTTP response is already sent.
    Uses a fresh client — NOT the request-scoped one which is already closed.
    """
    start_time = time.perf_counter()
    print(
        "Starting background task to materialise annotations for session:", session_id
    )
    session = get_background_client()

    session_info = get_session_status(session_id)

    set_session_status(
        session_id=session_id, status="processing", variant_count=variant_count
    )

    try:
        print(
            f"Materialising annotations for session {session_id} with {variant_count} variants..."
        )
        scores_table = f"nc_spark.scores_{session_info.genome}_normalized"
        gene_table = f"nc_spark.nearest_gene_{session_info.genome}"

        query = f"""
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

        print(scores_table, gene_table, query)

        session.command(
            query,
            parameters={"sid": session_id},
        )

        end_time = time.perf_counter()
        print(
            f"Completed materialisation for session {session_id} in {end_time - start_time:.2f} seconds."
        )
        set_session_status(
            session_id=session_id, status="complete", variant_count=variant_count
        )

    except Exception as e:
        error_client = get_background_client()
        set_session_status(
            status="error",
            error_message=str(e),
            session_id=session_id,
            variant_count=variant_count,
        )
        print(f"ERROR: Materialisation failed for session {session_id}. {e}")
        # Clean up partial data
        try:
            error_client.command(
                "ALTER TABLE nc_spark.user_uploads DELETE WHERE session_id = {sid:String}",
                parameters={"sid": session_id},
            )
            error_client.command(
                "ALTER TABLE nc_spark.user_results DELETE WHERE session_id = {sid:String}",
                parameters={"sid": session_id},
            )
            error_client.close()
        except Exception:
            pass
    finally:
        session.close()


def upload_variants(variants, genome, session, background_tasks: BackgroundTasks):
    """
    1. Inserts raw user variants into user_uploads.
    2. Materialises the annotation join into user_results (once, at upload time).
    3. Returns session_id so all subsequent filter/page requests hit the tiny pre-joined table.
    """
    if not variants:
        raise HTTPException(status_code=400, detail="No variants provided")

    session_id = str(uuid.uuid4())
    rows_to_insert = [(session_id, v.chr, v.pos, v.ref, v.alt) for v in variants]

    # ── Step 1: insert raw variants ───────────────────────────────────────────
    try:
        session.insert(
            "nc_spark.user_uploads",
            rows_to_insert,
            column_names=["session_id", "chr", "pos", "ref", "alt"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload variants: {e}")

    # ── Step 2: materialise annotation join into user_results ─────────────────
    set_session_status(
        genome=genome,
        status="pending",
        session_id=session_id,
        variant_count=len(rows_to_insert),
    )
    background_tasks.add_task(materialise_annotations, session_id, len(rows_to_insert))

    return {
        "session_id": session_id,
        "variant_count": len(rows_to_insert),
    }


# def upload_variants(variants, session):
#     """
#     Takes the user's variants, saves them to a temporary table, and returns a session_id.
#     """
#     if not variants:
#         raise HTTPException(status_code=400, detail="No variants provided")

#     session_id = str(uuid.uuid4())
#     rows_to_insert = [(session_id, v.chr, v.pos, v.ref, v.alt) for v in variants]

#     try:
#         session.insert(
#             "nc_spark.user_uploads",
#             rows_to_insert,
#             column_names=["session_id", "chr", "pos", "ref", "alt"],
#         )
#         return {"session_id": session_id, "variant_count": len(rows_to_insert)}
#     except Exception as e:
#         print(f"ERROR: Failed to insert user variants. {e}")
#         raise HTTPException(status_code=500, detail=f"Database insert error: {e}")


def get_filtered_variants(request, session):
    print(f"Received filter request for session_id: {request.session_id}")
    session_id = request.session_id
    page = getattr(request, "page", 1)
    page_size = getattr(request, "page_size", 20)
    offset = (page - 1) * page_size
    sort_by = getattr(request, "sort_by", None) or "chr"
    _sort_order = getattr(request, "sort_order", None) or "asc"
    sort_order = (
        _sort_order.value if hasattr(_sort_order, "value") else _sort_order
    ).upper()

    ALLOWED_SORT_COLUMNS = {"chr", "pos", "ref", "alt", "mean", "max", "CADD", "DANN"}
    ALLOWED_SORT_ORDERS = {"ASC", "DESC"}
    if sort_by not in ALLOWED_SORT_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid sort column: {sort_by}")
    if sort_order not in ALLOWED_SORT_ORDERS:
        raise HTTPException(status_code=400, detail=f"Invalid sort order: {sort_order}")

    try:
        total_results = session.command(
            """
            SELECT COUNT(*)
            FROM nc_spark.user_results
            WHERE session_id = {sid:String}
            """,
            parameters={"sid": session_id},
        )

        if total_results == 0:
            return {
                "results": [],
                "total_results": 0,
                "total_pages": 0,
                "page": page,
                "page_size": page_size,
            }

        main_result = session.query(
            f"""
            SELECT * EXCEPT (FATHMM_MKL_CODING, CSCAPE_CODING, FATHMM_XF_CODING)
            FROM nc_spark.user_results
            WHERE session_id = {{sid:String}}
            ORDER BY {sort_by} {sort_order}
            LIMIT {{page_size:UInt32}}
            OFFSET {{offset:UInt32}}
            """,
            parameters={"sid": session_id, "page_size": page_size, "offset": offset},
        )

        cols = main_result.column_names
        FLOAT_COLUMNS = {
            "GPN",
            "GERP",
            "NCER",
            "DANN",
            "CADD",
            "REMM",
            "FIRE",
            "ORION",
            "JARVIS",
            "GWRVIS",
            "FUNSEQ2",
            "LINSIGHT",
            "REGULOMEDB",
            "REPLISEQ_S2",
            "REPLISEQ_S4",
            "REPLISEQ_S1",
            "REPLISEQ_G2",
            "REPLISEQ_S3",
            "REPLISEQ_G1B",
            "PhyloP_30way",
            "PhyloP_100way",
            "regulatory_min",
            "regulatory_max",
            "MACIE_CONSERVED",
            "regulatory_mean",
            "CSCAPE_NONCODING",
            "MACIE_REGULATORY",
            "conservation_min",
            "conservation_max",
            "pathogenicity_min",
            "pathogenicity_max",
            "regulatory_median",
            "conservation_mean",
            "pathogenicity_mean",
            "FATHMM_XF_NONCODING",
            "conservation_median",
            "FATHMM_MKL_NONCODING",
            "pathogenicity_median",
            "replication_timing_min",
            "replication_timing_max",
            "replication_timing_mean",
            "replication_timing_median",
        }

        rows = [
            {
                k: round(v, 2) if k in FLOAT_COLUMNS and v is not None else v
                for k, v in dict(zip(cols, row)).items()
            }
            for row in main_result.result_rows
        ]

        return {
            "page": page,
            "results": rows,
            "page_size": page_size,
            "total_results": total_results,
            "total_pages": (total_results + page_size - 1) // page_size,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
