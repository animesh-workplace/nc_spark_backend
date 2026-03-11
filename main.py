import clickhouse_connect
from app.session import get_db
from typing import List, Dict, Any
from fastapi import BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Depends, APIRouter, HTTPException
from app.api.annotate import get_filtered_variants, upload_variants, get_session_status
from app.schema import (
    VariantInput,
    FilterRequest,
    UploadResponse,
    FilterResponse,
    StatusResponse,
    ScoreDistribution,
    DistributionBin,
    ReplicationRadarResponse,
    ALL_SCORES,
)

api_router = APIRouter()
BASE_URL = "/nvpp/api/v1"


@api_router.post("/upload_variants", response_model=UploadResponse)
def UPLOAD_VARIANTS(
    variants: List[VariantInput],
    background_tasks: BackgroundTasks,
    genome: str = "hg19",
    session: clickhouse_connect.driver.Client = Depends(get_db),
):
    return upload_variants(variants, genome, session, background_tasks)


@api_router.post("/get_filtered_variants", response_model=FilterResponse)
def GET_FILTERED_VARIANTS(
    request: FilterRequest,
    session: clickhouse_connect.driver.Client = Depends(get_db),
):
    return get_filtered_variants(request, session)


@api_router.get("/status/{session_id}", response_model=StatusResponse)
def GET_STATUS(session_id: str):
    session = get_session_status(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session.to_dict()


@api_router.get("/{session_id}/replication", response_model=ReplicationRadarResponse)
def GET_REPLICATION_RADAR(
    session_id: str,
    db: clickhouse_connect.driver.Client = Depends(get_db),
):
    result = db.query(
        """
        SELECT
            round(avg(REPLISEQ_S1),  4) AS S1,
            round(avg(REPLISEQ_S2),  4) AS S2,
            round(avg(REPLISEQ_S3),  4) AS S3,
            round(avg(REPLISEQ_S4),  4) AS S4,
            round(avg(REPLISEQ_G1B), 4) AS G1B,
            round(avg(REPLISEQ_G2),  4) AS G2
        FROM nc_spark.user_results
        WHERE session_id = {sid:String}
        """,
        parameters={"sid": session_id},
    )
    row = result.result_rows[0]
    return ReplicationRadarResponse(
        S1=row[0], S2=row[1], S3=row[2], S4=row[3], G1B=row[4], G2=row[5]
    )


@api_router.get("/{session_id}/distributions", response_model=list[ScoreDistribution])
def GET_DISTRIBUTIONS(
    session_id: str,
    bins: int = 20,
    db: clickhouse_connect.driver.Client = Depends(get_db),
):
    bin_width = round(1.0 / bins, 6)  # fixed 0.05 for bins=20

    distributions = []

    for score in ALL_SCORES:
        result = db.query(
            f"""
            SELECT
                round(floor({score} / {bin_width}) * {bin_width}, 6) AS bin_start,
                round(floor({score} / {bin_width}) * {bin_width} + {bin_width}, 6) AS bin_end,
                count() AS cnt
            FROM nc_spark.user_results
            WHERE session_id = {{sid:String}}
              AND {score} IS NOT NULL
              AND {score} >= 0
              AND {score} <= 1
            GROUP BY bin_start, bin_end
            ORDER BY bin_start
            """,
            parameters={"sid": session_id},
        )

        # Map returned bins by start value
        raw = {round(r[0], 6): r[2] for r in result.result_rows}

        # Always return exactly `bins` entries, filling missing with 0
        full_bins = [
            DistributionBin(
                bin_start=round(i * bin_width, 6),
                bin_end=round((i + 1) * bin_width, 6),
                count=raw.get(round(i * bin_width, 6), 0),
            )
            for i in range(bins)
        ]

        distributions.append(ScoreDistribution(score=score, bins=full_bins))

    return distributions


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
