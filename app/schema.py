from enum import Enum
from datetime import datetime
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Literal


class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


class VariantInput(BaseModel):
    chr: str
    pos: int
    ref: str
    alt: str


class FilterRange(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None


# This is the NEW model for the filtering endpoint
class FilterRequest(BaseModel):
    page: int = 1
    session_id: str
    sort_by: Optional[str] = "chr"
    page_size: int  # 20,50,100,200,500
    sort_order: Optional[SortOrder] = SortOrder.ASC
    filters: Optional[Dict[str, FilterRange]] = None


class UploadResponse(BaseModel):
    session_id: str
    variant_count: int


class FilterResponse(BaseModel):
    total_results: int
    results: List[Dict[str, Any]]
    total_pages: int
    page: int
    page_size: int


class StatusResponse(BaseModel):
    session_id: str
    status: str
    genome: str
    variant_count: int
    annotated_count: int
    vcf_hash: str | None
    error_message: str | None
    celery_task_id: str | None
    queue_name: str | None
    from_cache: bool
    created_at: datetime | None
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    expires_at: datetime | None


class RadarDataPoint(BaseModel):
    name: str
    value: List[float]


class ReplicationStats(BaseModel):
    early_score: float  # mean(S1, S2)
    late_score: float  # mean(S3, S4, G2)
    rt_score: float  # log2(early / late), canonical RT metric
    early_dominance_pct: float  # early_score / (early + late) * 100
    g1b_baseline: float  # G1B reference mean


class ReplicationRadarResponse(BaseModel):
    indicator: List[dict]
    series_data: List[RadarDataPoint]
    stats: ReplicationStats


class DistributionBin(BaseModel):
    bin_start: float
    bin_end: float
    count: int


class ScoreDistribution(BaseModel):
    score: str
    bins: list[DistributionBin]


PATHOGENICITY_SCORES = [
    "GPN",
    "DANN",
    "CADD",
    "ORION",
    "FATHMM_MKL_NONCODING",
    "CSCAPE_NONCODING",
    "FATHMM_XF_NONCODING",
]

CONSERVATION_SCORES = [
    "GERP",
    "NCER",
    "PhyloP_100way",
    "PhyloP_30way",
    "LINSIGHT",
    "JARVIS",
    "REMM",
    "MACIE_REGULATORY",
    "MACIE_CONSERVED",
    "GWRVIS",
]

REGULATORY_SCORES = [
    "FIRE",
    "FUNSEQ2",
    "REGULOMEDB",
]

REPLICATION_SCORES = [
    "REPLISEQ_S1",
    "REPLISEQ_S2",
    "REPLISEQ_S3",
    "REPLISEQ_S4",
    "REPLISEQ_G1B",
    "REPLISEQ_G2",
]

ALL_SCORES = (
    PATHOGENICITY_SCORES + CONSERVATION_SCORES + REGULATORY_SCORES + REPLICATION_SCORES
)


class BarChartResponse(BaseModel):
    categories: List[str]
    data: List[List[float]]
    mode: Literal["count", "frequency"]
