import uuid
from typing import Optional
from pydantic import BaseModel
from fastapi import HTTPException
from fastapi import BackgroundTasks


# utils.py
def set_session_status(
    session, session_id: str, status: str, variant_count: int = 0, error_msg: str = None
):
    session.command(
        """
        INSERT INTO nc_spark.session_status
            (session_id, status, variant_count, error_msg, updated_at)
        VALUES
            ({sid:String}, {status:String}, {count:UInt32}, 
             {err:Nullable(String)}, now())
        """,
        parameters={
            "sid": session_id,
            "status": status,
            "count": variant_count,
            "err": error_msg,
        },
    )


# tasks.py
def materialise_annotations(session_id: str, variant_count: int):
    """
    Runs after the HTTP response is already sent.
    Uses a fresh client — NOT the request-scoped one which is already closed.
    """
    from session import get_clickhouse_client_cached

    session = get_clickhouse_client_cached()

    set_session_status(session, session_id, "processing", variant_count)

    try:
        session.command(
            """
            INSERT INTO nc_spark.user_results
                (session_id, chr, pos, ref, alt,
                 GPN, GERP, NCER, DANN,
                 REPLISEQ_S2, REPLISEQ_G1B, REPLISEQ_S4, REPLISEQ_S1,
                 REPLISEQ_G2, REPLISEQ_S3,
                 FATHMM_MKL_CODING, FATHMM_MKL_NONCODING,
                 ORION, CSCAPE_NONCODING, CSCAPE_CODING,
                 CADD, PhyloP_100way, PhyloP_30way,
                 LINSIGHT, JARVIS, REMM, FIRE, FUNSEQ2,
                 FATHMM_XF_NONCODING, FATHMM_XF_CODING,
                 MACIE_REGULATORY, MACIE_CONSERVED,
                 REGULOMEDB, GWRVIS, mean, median, max, min)
            SELECT
                {sid:String} AS session_id, *
            FROM nc_spark.scores_hg19_normalized
            PREWHERE pos IN (
                SELECT DISTINCT pos FROM nc_spark.user_uploads
                WHERE session_id = {sid:String}
            )
            WHERE (chr, pos, ref, alt) IN (
                SELECT chr, pos, ref, alt FROM nc_spark.user_uploads
                WHERE session_id = {sid:String}
            )
            SETTINGS join_algorithm = 'hash',
                     max_execution_time = 600    -- 10 min ceiling for 7 lakh rows
            """,
            parameters={"sid": session_id},
        )
        set_session_status(session, session_id, "complete", variant_count)

    except Exception as e:
        set_session_status(session, session_id, "error", variant_count, str(e))
        # Clean up partial data
        try:
            session.command(
                "ALTER TABLE nc_spark.user_uploads DELETE WHERE session_id = {sid:String}",
                parameters={"sid": session_id},
            )
            session.command(
                "ALTER TABLE nc_spark.user_results DELETE WHERE session_id = {sid:String}",
                parameters={"sid": session_id},
            )
        except Exception:
            pass


def upload_variants(variants, session):
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
    try:
        session.command(
            """
            INSERT INTO nc_spark.user_results
                (session_id, chr, pos, ref, alt,
                GPN, GERP, NCER, DANN,
                REPLISEQ_S2, REPLISEQ_G1B, REPLISEQ_S4, REPLISEQ_S1, REPLISEQ_G2, REPLISEQ_S3,
                FATHMM_MKL_CODING, FATHMM_MKL_NONCODING,
                ORION, CSCAPE_NONCODING, CSCAPE_CODING,
                CADD, PhyloP_100way, PhyloP_30way,
                LINSIGHT, JARVIS, REMM, FIRE, FUNSEQ2,
                FATHMM_XF_NONCODING, FATHMM_XF_CODING,
                MACIE_REGULATORY, MACIE_CONSERVED,
                REGULOMEDB, GWRVIS,
                mean, median, max, min)   -- created_at intentionally omitted → uses DEFAULT now()
            SELECT
                {sid:String} AS session_id,
                *
            FROM nc_spark.scores_hg19_normalized
            PREWHERE pos IN (
                SELECT DISTINCT pos
                FROM nc_spark.user_uploads
                WHERE session_id = {sid:String}
            )
            WHERE (chr, pos, ref, alt) IN (
                SELECT chr, pos, ref, alt
                FROM nc_spark.user_uploads
                WHERE session_id = {sid:String}
            )
            SETTINGS join_algorithm = 'hash',
                    max_execution_time = 120
            """,
            parameters={"sid": session_id},
        )
    except Exception as e:
        # Clean up the raw upload so the session isn't left in a half-ready state
        try:
            session.command(
                "ALTER TABLE nc_spark.user_uploads DELETE WHERE session_id = {sid:String}",
                parameters={"sid": session_id},
            )
        except Exception:
            pass  # best-effort cleanup, don't mask the original error
        raise HTTPException(
            status_code=500, detail=f"Failed to materialise annotations: {e}"
        )

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
    session_id = request.session_id
    page = getattr(request, "page", 1)
    page_size = getattr(request, "page_size", 20)
    offset = (page - 1) * page_size
    sort_by = getattr(request, "sort_by", None) or "chr"
    # Always call .value if it's an enum, then uppercase for SQL
    _sort_order = getattr(request, "sort_order", None) or "asc"
    sort_order = (
        _sort_order.value if hasattr(_sort_order, "value") else _sort_order
    ).upper()

    try:
        # ── Phase 1: fetch the 99 user variants (tiny, fast) ──────────────────
        user_variants_result = session.query(
            """
            SELECT chr, pos, ref, alt
            FROM nc_spark.user_uploads
            WHERE session_id = {sid:String}
            """,
            parameters={"sid": session_id},
        )
        user_rows = user_variants_result.result_rows  # list of (chr, pos, ref, alt)
        print(f"User has {len(user_rows)} uploaded variants for session {session_id}.")

        if not user_rows:
            return {
                "data": [],
                "total_count": 0,
                "total_pages": 0,
                "page": page,
                "page_size": page_size,
            }

        # ── Phase 2: use extracted positions to guide primary key index ────────
        main_result = session.query(
            f"""
            SELECT *
            FROM nc_spark.scores_hg19_normalized
            PREWHERE pos IN (
                SELECT DISTINCT pos
                FROM nc_spark.user_uploads
                WHERE session_id = {{sid:String}}
            )
            WHERE (chr, pos, ref, alt) IN (
                SELECT chr, pos, ref, alt
                FROM nc_spark.user_uploads
                WHERE session_id = {{sid:String}}
            )
            ORDER BY {sort_by} {sort_order}
            LIMIT {{page_size:UInt32}}
            OFFSET {{offset:UInt32}}
            """,
            parameters={
                "sid": session_id,
                "page_size": page_size,
                "offset": offset,
            },
            settings={
                "join_algorithm": "hash",
                "max_execution_time": 30,
            },
        )

        total_count = len(user_rows)  # already known from phase 1
        total_pages = (total_count + page_size - 1) // page_size
        cols = main_result.column_names
        rows = [dict(zip(cols, row)) for row in main_result.result_rows]

        return {
            "page": page,
            "results": rows,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_results": total_count,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")
