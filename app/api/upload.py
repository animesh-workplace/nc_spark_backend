import os
import uuid
import shutil
from pathlib import Path
import fireducks.pandas as pd
from fastapi import HTTPException, UploadFile
from app.api.annotation import materialise_annotations
from app.api.job_status import create_or_update_job_status

# from app.api.annotate import set_session_status,materialise_annotations
# from priority import _variant_priority, _variant_queue

REQUIRED_COLUMNS = {"chr", "pos", "ref", "alt"}
UPLOAD_DIR = Path("media")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def upload_variants2(file: UploadFile, genome: str, file_format: str, session) -> dict:

    session_id = str(uuid.uuid4())
    dest_path = UPLOAD_DIR / f"{session_id}.{file_format}"

    # Step 1: Stream directly to persistent destination
    try:
        with dest_path.open("wb") as out:
            shutil.copyfileobj(file.file, out)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")

    # Step 2: Load from saved path with FireDucks
    sep = "\t" if file_format == "tsv" else ","

    try:
        df = pd.read_csv(
            str(dest_path),
            sep=sep,
            usecols=lambda col: col.strip().lower() in REQUIRED_COLUMNS,
            dtype={"chr": str, "ref": str, "alt": str, "pos": int},
        )
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse file: {e}")

    # Step 3: Normalise column names
    df.columns = [c.strip().lower() for c in df.columns]

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"Missing required columns: {missing}. Found: {list(df.columns)}",
        )

    # Step 4: Drop incomplete rows
    df = df[["chr", "pos", "ref", "alt"]].dropna()
    df["pos"] = df["pos"].astype(int)

    if df.empty:
        raise HTTPException(
            status_code=400, detail="No valid variant rows found after parsing."
        )

    # Step 5: Build insert rows
    valid_chroms = {str(i) for i in range(1, 23)} | {"X", "Y"}

    rows_to_insert = [
        (
            session_id,
            f"chr{row.chr}" if str(row.chr) in valid_chroms else row.chr,
            row.pos,
            row.ref,
            row.alt,
        )
        for row in df.itertuples(index=False)
    ]
    variant_count = len(rows_to_insert)

    # Step 6: Insert into ClickHouse
    try:
        session.insert(
            "nc_spark.user_uploads",
            rows_to_insert,
            column_names=["session_id", "chr", "pos", "ref", "alt"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload variants: {e}")

    # Step 7: Set session status
    create_or_update_job_status(
        genome=genome,
        status="pending",
        session_id=session_id,
        variant_count=variant_count,
        stored_file_path=str(dest_path),
    )

    # Step 8: Compute priority + dispatch
    # priority = _variant_priority(variant_count)
    # queue = _variant_queue(variant_count)

    # result = materialise_annotations.apply_async(
    #     args=[session_id, variant_count],
    #     priority=priority,
    #     queue=queue,
    # )

    result = materialise_annotations.apply_async(
        args=[session_id, variant_count],
    )

    create_or_update_job_status(
        status="pending",
        session_id=session_id,
        celery_task_id=result.id,
        # current_priority=priority,
    )

    return {
        "session_id": session_id,
        "variant_count": variant_count,
    }
