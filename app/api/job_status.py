from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from app.session import get_local_background_client


_STATUS_TIMESTAMP = {
    "queued": "queued_at",
    "processing": "started_at",
    "complete": "completed_at",
    "error": "completed_at",
}

TABLE = "nc_spark.session_status"


@dataclass
class JobStatus:
    """Python representation of a session_status row."""

    session_id: str
    version: int = 0
    genome: str = "hg19"
    variant_count: int = 0
    status: str = "pending"
    annotated_count: int = 0
    from_cache: bool = False
    vcf_hash: Optional[str] = None
    queue_name: Optional[str] = None
    error_message: Optional[str] = None
    celery_task_id: Optional[str] = None
    queued_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    current_priority: Optional[int] = None
    stored_file_path: Optional[str] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "genome": self.genome,
            "vcf_hash": self.vcf_hash,
            "session_id": self.session_id,
            "from_cache": self.from_cache,
            "queue_name": self.queue_name,
            "variant_count": self.variant_count,
            "error_message": self.error_message,
            "celery_task_id": self.celery_task_id,
            "annotated_count": self.annotated_count,
            "current_priority": self.current_priority,
            "stored_file_path": self.stored_file_path,
            "queued_at": self.queued_at.isoformat() if self.queued_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
        }


def get_job_status(session_id: str) -> JobStatus | None:
    """
    Fetches the latest version of a session using FINAL.
    Returns None if not found.
    """
    client = get_local_background_client()
    try:
        result = client.query(
            f"""
                SELECT * FROM {TABLE} FINAL
                WHERE session_id = {{sid:String}} LIMIT 1
            """,
            parameters={"sid": session_id},
        )
        if not result.result_rows:
            return None

        row = dict(zip(result.column_names, result.result_rows[0]))
        return _row_to_job_status(row)
    finally:
        client.close()


def create_or_update_job_status(
    session_id: str,
    *,
    # Create-only fields
    genome: str = None,
    ttl_hours: int = 24,
    vcf_hash: str = None,
    queue_name: str = None,
    variant_count: int = None,
    stored_file_path: str = None,
    # Update fields
    status: str = None,
    from_cache: bool = False,
    error_message: str = None,
    celery_task_id: str = None,
    annotated_count: int = None,
    current_priority: int = None,
) -> JobStatus:
    """
    Upserts a session_status row.
    - On first call: inserts a new row at version=0.
    - On subsequent calls: fetches the current row, increments version,
      merges changed fields, and inserts the new version.
    ClickHouse ReplacingMergeTree keeps only the highest version per session_id.
    """
    client = get_local_background_client()
    now = datetime.now(timezone.utc)

    try:
        existing = get_job_status(session_id)

        if existing is None:
            # CREATE
            row = JobStatus(
                version=0,
                created_at=now,
                annotated_count=0,
                vcf_hash=vcf_hash,
                session_id=session_id,
                from_cache=from_cache,
                genome=genome or "hg19",
                status=status or "pending",
                celery_task_id=celery_task_id,
                variant_count=variant_count or 0,
                queue_name=queue_name or "large",
                current_priority=current_priority,
                stored_file_path=stored_file_path,
                expires_at=now + timedelta(hours=ttl_hours),
            )
            if status and (ts_field := _STATUS_TIMESTAMP.get(status)):
                setattr(row, ts_field, now)

        else:
            # UPDATE
            row = existing
            row.version += 1  # critical — drives ReplacingMergeTree dedup

            if status is not None:
                row.status = status
                if ts_field := _STATUS_TIMESTAMP.get(status):
                    setattr(row, ts_field, now)

            if celery_task_id is not None:
                row.celery_task_id = celery_task_id
            if current_priority is not None:
                row.current_priority = current_priority
            if annotated_count is not None:
                row.annotated_count = annotated_count
            if error_message is not None:
                row.error_message = error_message
            if stored_file_path is not None:
                row.stored_file_path = stored_file_path
            if from_cache:
                row.from_cache = True

        _insert_row(client, row)
        return row

    finally:
        client.close()


# Internal helpers
def _insert_row(client, row: JobStatus):
    """Inserts a single JobStatus row into ClickHouse."""
    client.insert(
        TABLE,
        [
            [
                row.session_id,
                row.version,
                row.status,
                row.genome,
                row.variant_count,
                row.annotated_count,
                row.vcf_hash,
                int(row.from_cache),
                row.error_message,
                row.celery_task_id,
                row.queue_name,
                row.current_priority,
                row.stored_file_path,
                row.created_at or datetime.now(timezone.utc),
                row.queued_at,
                row.started_at,
                row.completed_at,
                row.expires_at or (datetime.now(timezone.utc) + timedelta(hours=24)),
            ]
        ],
        column_names=[
            "session_id",
            "version",
            "status",
            "genome",
            "variant_count",
            "annotated_count",
            "vcf_hash",
            "from_cache",
            "error_message",
            "celery_task_id",
            "queue_name",
            "current_priority",
            "stored_file_path",
            "created_at",
            "queued_at",
            "started_at",
            "completed_at",
            "expires_at",
        ],
    )


def _row_to_job_status(row: dict) -> JobStatus:
    return JobStatus(
        status=row["status"],
        genome=row["genome"],
        version=row["version"],
        session_id=row["session_id"],
        vcf_hash=row.get("vcf_hash"),
        queued_at=row.get("queued_at"),
        queue_name=row.get("queue_name"),
        created_at=row.get("created_at"),
        started_at=row.get("started_at"),
        expires_at=row.get("expires_at"),
        variant_count=row["variant_count"],
        completed_at=row.get("completed_at"),
        annotated_count=row["annotated_count"],
        error_message=row.get("error_message"),
        celery_task_id=row.get("celery_task_id"),
        from_cache=bool(row.get("from_cache", 0)),
        current_priority=row.get("current_priority"),
        stored_file_path=row.get("stored_file_path"),
    )
