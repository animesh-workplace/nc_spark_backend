import math
import clickhouse_connect
from app.session import get_db
from typing import List, Literal
from fastapi import BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Depends, APIRouter, HTTPException
from app.api.annotate import get_filtered_variants, upload_variants, get_session_status
from app.schema import (
    ALL_SCORES,
    VariantInput,
    FilterRequest,
    UploadResponse,
    FilterResponse,
    StatusResponse,
    RadarDataPoint,
    DistributionBin,
    ReplicationStats,
    BarChartResponse,
    ScoreDistribution,
    ReplicationRadarResponse,
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


@api_router.get(
    "/{session_id}/variants-per-chromosome", response_model=BarChartResponse
)
def GET_VARIANTS_PER_CHROMOSOME(
    session_id: str,
    mode: Literal[
        "count", "frequency"
    ] = "count",  # count makes more sense as default here
    db: clickhouse_connect.driver.Client = Depends(get_db),
):
    result = db.query(
        """
        SELECT
            chr,
            count() AS cnt
        FROM nc_spark.user_results
        WHERE session_id = {sid:String}
          AND chr IS NOT NULL
          AND chr != ''
        GROUP BY chr
        """,
        parameters={"sid": session_id},
    )

    rows = result.result_rows

    if not rows:
        return BarChartResponse(categories=[], data=[[]], mode=mode)

    categories = [row[0] for row in rows]
    counts = [row[1] for row in rows]

    if mode == "frequency":
        total = sum(counts)
        values = [round(c / total, 4) for c in counts]
    else:
        values = [float(c) for c in counts]

    # Natural sort so chr1 < chr2 < ... < chr10 < chrX < chrY < chrMT
    def chrom_sort_key(pair):
        chrom = pair[0].replace("chr", "").replace("CHR", "")
        order = {"X": 23, "Y": 24, "MT": 25, "M": 25}
        try:
            return order.get(chrom, int(chrom))
        except ValueError:
            return 99  # unknown contigs go last

    sorted_pairs = sorted(zip(categories, values), key=chrom_sort_key)
    categories, values = map(list, zip(*sorted_pairs))

    return BarChartResponse(categories=categories, data=[values], mode=mode)


@api_router.get("/{session_id}/trinucleotide", response_model=BarChartResponse)
def GET_TRINUCLEOTIDE_BARCHART(
    session_id: str,
    mode: Literal["count", "frequency"] = "frequency",
    db: clickhouse_connect.driver.Client = Depends(get_db),
):
    result = db.query(
        """
        SELECT
            trinucleotide,
            count() AS cnt
        FROM nc_spark.user_results
        WHERE session_id = {sid:String}
          AND trinucleotide IS NOT NULL
          AND trinucleotide != ''
        GROUP BY trinucleotide
        """,
        parameters={"sid": session_id},
    )

    rows = result.result_rows

    if not rows:
        return BarChartResponse(categories=[], data=[[]], mode=mode)

    categories = [row[0] for row in rows]
    counts = [row[1] for row in rows]

    if mode == "frequency":
        total = sum(counts)
        values = [round((c / total) * 100, 2) for c in counts]
    else:
        values = [float(c) for c in counts]

    # Sort by value descending
    sorted_pairs = sorted(zip(categories, values), key=lambda x: x[1], reverse=True)
    categories, values = map(list, zip(*sorted_pairs))

    return BarChartResponse(categories=categories, data=[values], mode=mode)


@api_router.get("/{session_id}/snv-change", response_model=BarChartResponse)
def GET_SNV_CHANGE_BARCHART(
    session_id: str,
    mode: Literal["count", "frequency"] = "frequency",
    db: clickhouse_connect.driver.Client = Depends(get_db),
):
    result = db.query(
        """
        SELECT
            concat(ref, '>', alt) AS snv_change,
            count()               AS cnt
        FROM nc_spark.user_results
        WHERE session_id = {sid:String}
          AND ref IS NOT NULL AND ref != ''
          AND alt IS NOT NULL AND alt != ''
          AND length(ref) = 1
          AND length(alt) = 1
        GROUP BY snv_change
        """,
        parameters={"sid": session_id},
    )

    rows = result.result_rows

    if not rows:
        return BarChartResponse(categories=[], data=[[]], mode=mode)

    categories = [row[0] for row in rows]
    counts = [row[1] for row in rows]

    if mode == "frequency":
        total = sum(counts)
        values = [round(c / total, 4) for c in counts]
    else:
        values = [float(c) for c in counts]

    sorted_pairs = sorted(zip(categories, values), key=lambda x: x[1], reverse=True)
    categories, values = map(list, zip(*sorted_pairs))

    return BarChartResponse(categories=categories, data=[values], mode=mode)


@api_router.get("/{session_id}/replication", response_model=ReplicationRadarResponse)
def GET_REPLICATION_RADAR(
    session_id: str,
    db: clickhouse_connect.driver.Client = Depends(get_db),
):
    indicator = [
        {"name": "G1B"},
        {"name": "S1"},
        {"name": "S2"},
        {"name": "S3"},
        {"name": "S4"},
        {"name": "G2"},
    ]

    result = db.query(
        """
        SELECT
            REPLISEQ_G1B,
            REPLISEQ_S1,
            REPLISEQ_S2,
            REPLISEQ_S3,
            REPLISEQ_S4,
            REPLISEQ_G2
        FROM nc_spark.user_results
        WHERE session_id = {sid:String}
        """,
        parameters={"sid": session_id},
    )

    rows = result.result_rows

    # Guard: no data found for this session_id
    if not rows:
        return ReplicationRadarResponse(indicator=indicator, series_data=[])

    series_data = [
        RadarDataPoint(
            name=f"Row {i + 1}",
            value=[round(v, 4) if v is not None else 0.0 for v in row],
        )
        for i, row in enumerate(rows)
    ]

    def safe_mean(vals):
        clean = [v for v in vals if v is not None]
        return round(sum(clean) / len(clean), 4) if clean else 0.0

    def safe_median(vals):
        s = sorted(v for v in vals if v is not None)
        n = len(s)
        if not s:
            return 0.0
        mid = n // 2
        return round((s[mid] if n % 2 != 0 else (s[mid - 1] + s[mid]) / 2), 4)

    means = [safe_mean([row[i] for row in rows]) for i in range(6)]
    medians = [safe_median([row[i] for row in rows]) for i in range(6)]

    series_data.append(RadarDataPoint(name="Mean", value=means))
    series_data.append(RadarDataPoint(name="Median", value=medians))

    phase_names = ["G1B", "S1", "S2", "S3", "S4", "G2"]
    means_by_name = dict(zip(phase_names, means))

    early_score = safe_mean([means_by_name["S1"], means_by_name["S2"]])
    late_score = safe_mean(
        [means_by_name["S3"], means_by_name["S4"], means_by_name["G2"]]
    )
    rt_score = (
        round(math.log2(early_score / late_score), 4)
        if late_score > 0 and early_score > 0
        else 0.0
    )
    early_dom = (
        round((early_score / (early_score + late_score)) * 100, 2)
        if (early_score + late_score) > 0
        else 0.0
    )

    stats = ReplicationStats(
        early_score=early_score,
        late_score=late_score,
        rt_score=rt_score,
        early_dominance_pct=early_dom,
        g1b_baseline=means_by_name["G1B"],
    )

    return ReplicationRadarResponse(
        indicator=indicator, series_data=series_data, stats=stats
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
