from enum import Enum
from pydantic import BaseModel
from typing import List, Dict, Any, Optional


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
    variant_count: int
    error_msg: Optional[str] = None
