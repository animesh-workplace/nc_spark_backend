import clickhouse_connect
from app.session import get_db
from typing import List, Dict, Any
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Depends, APIRouter, HTTPException
from app.api.annotate import get_filtered_variants, upload_variants
from app.schema import (
    VariantInput,
    FilterRequest,
    UploadResponse,
    FilterResponse,
    StatusResponse,
)

api_router = APIRouter()
BASE_URL = "/nvpp/api/v1"


@api_router.post("/upload_variants", response_model=UploadResponse)
def UPLOAD_VARIANTS(
    variants: List[VariantInput],
    session: clickhouse_connect.driver.Client = Depends(get_db),
):
    return upload_variants(variants, session)


@api_router.post("/get_filtered_variants", response_model=FilterResponse)
def GET_FILTERED_VARIANTS(
    request: FilterRequest,
    session: clickhouse_connect.driver.Client = Depends(get_db),
):
    return get_filtered_variants(request, session)


@api_router.get("/status/{session_id}", response_model=StatusResponse)
def GET_STATUS(
    session_id: str,
    session: clickhouse_connect.driver.Client = Depends(get_db),
):
    result = session.query(
        """
        SELECT session_id, status, variant_count, error_msg
        FROM nc_spark.session_status
        FINAL
        WHERE session_id = {sid:String}
        LIMIT 1
        """,
        parameters={"sid": session_id},
    )
    if not result.result_rows:
        raise HTTPException(status_code=404, detail="Session not found")

    cols = result.column_names
    row = dict(zip(cols, result.result_rows[0]))
    return row


app = FastAPI(
    version="1.0.0",
    title="Variant Annotation API",
    description="An API to batch-annotate variants against a ClickHouse database.",
)
origins = ["http://10.10.6.80", "*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
)
app.include_router(api_router, prefix=BASE_URL)
