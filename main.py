import math
import clickhouse_connect
from typing import List, Literal
from collections import defaultdict
from app.session import get_local_db
from app.api.upload import upload_variants2
from app.api.job_status import get_job_status
from fastapi.middleware.cors import CORSMiddleware
from app.api.annotate import get_filtered_variants
from fastapi import FastAPI, Depends, APIRouter, HTTPException, File, Form, UploadFile
from app.schema import (
    ALL_SCORES,
    BoxplotStats,
    TiTvResponse,
    FilterRequest,
    UploadResponse,
    FilterResponse,
    StatusResponse,
    RadarDataPoint,
    DistributionBin,
    VariantScoreRow,
    ReplicationStats,
    BarChartResponse,
    GroupTopVariants,
    ScoreDistribution,
    CrossGroupVariant,
    TopVariantsResponse,
    ReplicationRadarResponse,
)

api_router = APIRouter()
BASE_URL = "/nvpp/api/v1"


@api_router.post("/upload", response_model=UploadResponse)
def UPLOAD_VARIANTS(
    file: UploadFile = File(...),
    genome: str = Form(...),
    file_format: Literal["csv", "tsv"] = Form(...),
    session: clickhouse_connect.driver.Client = Depends(get_local_db),
):
    return upload_variants2(
        file=file,
        genome=genome,
        session=session,
        file_format=file_format,
    )


@api_router.get("/status/{session_id}", response_model=StatusResponse)
def GET_STATUS(session_id: str):
    session = get_job_status(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session.to_dict()


@api_router.post("/get_filtered_variants", response_model=FilterResponse)
def GET_FILTERED_VARIANTS(
    request: FilterRequest,
    session: clickhouse_connect.driver.Client = Depends(get_local_db),
):
    return get_filtered_variants(request, session)


#############################################################################


# @api_router.post("/upload_variants", response_model=UploadResponse)
# def UPLOAD_VARIANTS(
#     variants: List[VariantInput],
#     background_tasks: BackgroundTasks,
#     genome: str = "hg19",
#     session: clickhouse_connect.driver.Client = Depends(get_remote_db),
# ):
#     return upload_variants(variants, genome, session, background_tasks)


def compute_boxplot_stats(values: List[float]) -> BoxplotStats:
    s = sorted(values)

    def percentile(data, pct):
        idx = (len(data) - 1) * pct / 100
        lo, hi = int(idx), min(int(idx) + 1, len(data) - 1)
        return round(data[lo] + (data[hi] - data[lo]) * (idx - lo), 4)

    return BoxplotStats(
        min=round(s[0], 4),
        q1=percentile(s, 25),
        median=percentile(s, 50),
        q3=percentile(s, 75),
        max=round(s[-1], 4),
    )


TRANSITIONS = {"A>G", "G>A", "C>T", "T>C"}
TRANSVERSIONS = {"A>C", "C>A", "A>T", "T>A", "G>C", "C>G", "G>T", "T>G"}
SCORE_GROUPS = {
    "pathogenicity": [
        "CADD",
        "CSCAPE_NONCODING",
        "DANN",
        "FATHMM_MKL_NONCODING",
        "FATHMM_XF_NONCODING",
        "GPN",
        "GWRVIS",
        "JARVIS",
        "LINSIGHT",
        "NCER",
        "ORION",
        "REMM",
    ],
    "regulatory": ["FUNSEQ2", "FIRE", "REGULOMEDB", "MACIE_REGULATORY"],
    "conservation": ["GERP", "PhyloP_100way", "PhyloP_30way", "MACIE_CONSERVED"],
    "replication_timing": [
        "REPLISEQ_S1",
        "REPLISEQ_S2",
        "REPLISEQ_S3",
        "REPLISEQ_S4",
        "REPLISEQ_G1B",
        "REPLISEQ_G2",
    ],
}

# Maps group key → actual column prefix in the DB
GROUP_STAT_NAMES = {
    "pathogenicity": "pathogenicity",
    "regulatory": "regulatory",
    "conservation": "conservation",
    "replication_timing": "replication_timing",
}


@api_router.get("/{session_id}/top-variants", response_model=TopVariantsResponse)
def GET_TOP_VARIANTS(
    session_id: str,
    rank_by: Literal["mean", "median", "min", "max"] = "mean",
    limit: int = 10,
    db: clickhouse_connect.driver.Client = Depends(get_local_db),
):
    results = {}
    variant_group_map = defaultdict(list)  # variant → [group names it appears in]
    variant_gene_map: dict = {}  # variant → gene annotation fields (first seen)

    for group in SCORE_GROUPS:
        rank_col = f"{GROUP_STAT_NAMES[group]}_{rank_by}"

        query = f"""
            SELECT
                concat(chr, ':', toString(pos), ':', ref, '>', alt) AS variant,
                round({rank_col}, 4) AS group_score,
                gene_if_overlapping,
                nearest_gene_plus,
                plus_distance,
                nearest_gene_minus,
                minus_distance
            FROM nc_spark.user_results
            WHERE session_id = {{sid:String}}
              AND {rank_col} IS NOT NULL
            ORDER BY {rank_col} DESC
            LIMIT {{lim:UInt32}}
        """

        result = db.query(
            query,
            parameters={"sid": session_id, "lim": limit},
        )

        top_rows = [
            VariantScoreRow(
                variant=row[0],
                group_score=row[1],
                gene_if_overlapping=row[2] or "",
                nearest_gene_plus=row[3] or "",
                plus_distance=row[4],
                nearest_gene_minus=row[5] or "",
                minus_distance=row[6],
            )
            for row in result.result_rows
        ]

        # Track which groups each variant appears in
        for row in top_rows:
            variant_group_map[row.variant].append(group)
            # Store gene annotation on first encounter
            if row.variant not in variant_gene_map:
                variant_gene_map[row.variant] = {
                    "gene_if_overlapping": row.gene_if_overlapping,
                    "nearest_gene_plus": row.nearest_gene_plus,
                    "plus_distance": row.plus_distance,
                    "nearest_gene_minus": row.nearest_gene_minus,
                    "minus_distance": row.minus_distance,
                }

        results[group] = GroupTopVariants(
            group=group,
            rank_by=rank_by,
            rank_col=rank_col,
            top=top_rows,
        )

    # Build cross-group hits — only variants appearing in 2+ groups
    cross_group_hits = sorted(
        [
            CrossGroupVariant(
                variant=variant,
                appears_in=groups,
                group_count=len(groups),
                **variant_gene_map.get(variant, {}),
            )
            for variant, groups in variant_group_map.items()
            if len(groups) >= 2
        ],
        key=lambda x: x.group_count,
        reverse=True,
    )

    return TopVariantsResponse(results=results, cross_group_hits=cross_group_hits)


@api_router.get("/{session_id}/titv", response_model=TiTvResponse)
def GET_TITV(
    session_id: str,
    mode: Literal["count", "frequency", "boxplot"] = "count",
    db: clickhouse_connect.driver.Client = Depends(get_local_db),
):
    # Base query — for boxplot fetch a numeric score to distribute,
    # for count/frequency we just need the SNV change + count
    if mode == "boxplot":
        result = db.query(
            """
            SELECT
                concat(ref, '>', alt) AS snv_change,
                CADD AS score
            FROM nc_spark.user_results
            WHERE session_id = {sid:String}
              AND ref IS NOT NULL AND ref  != ''
              AND alt IS NOT NULL AND alt  != ''
              AND length(ref) = 1
              AND length(alt) = 1
              AND CADD IS NOT NULL
            """,
            parameters={"sid": session_id},
        )
        rows = result.result_rows

        ti_scores, tv_scores = [], []
        for snv_change, score in rows:
            if snv_change in TRANSITIONS:
                ti_scores.append(score)
            elif snv_change in TRANSVERSIONS:
                tv_scores.append(score)

        ti_count = len(ti_scores)
        tv_count = len(tv_scores)
        titv_ratio = round(ti_count / tv_count, 4) if tv_count > 0 else 0.0

        boxplot = {}
        if ti_scores:
            boxplot["Ti"] = compute_boxplot_stats(ti_scores)
        if tv_scores:
            boxplot["Tv"] = compute_boxplot_stats(tv_scores)

        return TiTvResponse(
            categories=["Ti", "Tv"],
            data=[[]],
            boxplot=boxplot,
            mode=mode,
            ti_count=ti_count,
            tv_count=tv_count,
            titv_ratio=titv_ratio,
        )

    # count / frequency mode — same as before
    result = db.query(
        """
        SELECT
            concat(ref, '>', alt) AS snv_change,
            count() AS cnt
        FROM nc_spark.user_results
        WHERE session_id = {sid:String}
          AND ref IS NOT NULL AND ref  != ''
          AND alt IS NOT NULL AND alt  != ''
          AND length(ref) = 1
          AND length(alt) = 1
        GROUP BY snv_change
        """,
        parameters={"sid": session_id},
    )
    rows = result.result_rows

    if not rows:
        return TiTvResponse(
            categories=["Ti", "Tv"],
            data=[[0.0, 0.0]],
            boxplot=None,
            mode=mode,
            ti_count=0,
            tv_count=0,
            titv_ratio=0.0,
        )

    ti_count, tv_count = 0, 0
    for snv_change, cnt in rows:
        if snv_change in TRANSITIONS:
            ti_count += cnt
        elif snv_change in TRANSVERSIONS:
            tv_count += cnt

    total = ti_count + tv_count
    titv_ratio = round(ti_count / tv_count, 4) if tv_count > 0 else 0.0

    if mode == "frequency":
        values = [
            round((ti_count / total) * 100, 2) if total > 0 else 0.0,
            round((tv_count / total) * 100, 2) if total > 0 else 0.0,
        ]
    else:
        values = [float(ti_count), float(tv_count)]

    return TiTvResponse(
        categories=["Ti", "Tv"],
        data=[values],
        boxplot=None,
        mode=mode,
        ti_count=ti_count,
        tv_count=tv_count,
        titv_ratio=titv_ratio,
    )


@api_router.get(
    "/{session_id}/variants-per-chromosome", response_model=BarChartResponse
)
def GET_VARIANTS_PER_CHROMOSOME(
    session_id: str,
    mode: Literal[
        "count", "frequency"
    ] = "count",  # count makes more sense as default here
    db: clickhouse_connect.driver.Client = Depends(get_local_db),
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
        values = [round((c / total) * 100, 2) for c in counts]
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

    sorted_pairs = sorted(zip(categories, values), key=lambda x: x[1], reverse=True)
    categories, values = map(list, zip(*sorted_pairs))

    return BarChartResponse(categories=categories, data=[values], mode=mode)


@api_router.get("/{session_id}/trinucleotide", response_model=BarChartResponse)
def GET_TRINUCLEOTIDE_BARCHART(
    session_id: str,
    mode: Literal["count", "frequency"] = "frequency",
    db: clickhouse_connect.driver.Client = Depends(get_local_db),
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
    db: clickhouse_connect.driver.Client = Depends(get_local_db),
):
    result = db.query(
        """
        SELECT
            concat(ref, '>', alt) AS snv_change,
            count() AS cnt
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
        values = [round((c / total) * 100, 2) for c in counts]
    else:
        values = [float(c) for c in counts]

    sorted_pairs = sorted(zip(categories, values), key=lambda x: x[1], reverse=True)
    categories, values = map(list, zip(*sorted_pairs))

    return BarChartResponse(categories=categories, data=[values], mode=mode)


@api_router.get("/{session_id}/replication", response_model=ReplicationRadarResponse)
def GET_REPLICATION_RADAR(
    session_id: str,
    db: clickhouse_connect.driver.Client = Depends(get_local_db),
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
            value=[
                round(v, 4) if (v is not None and math.isfinite(v)) else 0.0
                for v in row
            ],
        )
        for i, row in enumerate(rows)
    ]

    def is_finite(x: float | None) -> bool:
        return x is not None and math.isfinite(x)

    def safe_mean(vals):
        clean = [v for v in vals if is_finite(v)]
        return round(sum(clean) / len(clean), 4) if clean else 0.0

    def safe_median(vals):
        s = sorted(v for v in vals if is_finite(v))
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

    def safe_log2_ratio(num: float, den: float) -> float:
        if num <= 0 or den <= 0:
            return 0.0
        ratio = num / den
        if not math.isfinite(ratio):
            return 0.0
        v = math.log2(ratio)
        return round(v, 4) if math.isfinite(v) else 0.0

    rt_score = safe_log2_ratio(early_score, late_score)

    def safe_pct(num: float, den: float) -> float:
        total = num + den
        if total <= 0 or not math.isfinite(total):
            return 0.0
        v = (num / total) * 100
        return round(v, 2) if math.isfinite(v) else 0.0

    early_dom = safe_pct(early_score, late_score)

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
    db: clickhouse_connect.driver.Client = Depends(get_local_db),
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
