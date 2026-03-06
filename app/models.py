import uuid
from app.session import Base
from datetime import datetime, timezone, timedelta
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, Index


def _now():
    return datetime.now(timezone.utc)


def _expires():
    return datetime.now(timezone.utc) + timedelta(hours=24)


class SessionStatus(Base):
    __tablename__ = "session_status"

    session_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String(20), nullable=False, default="pending")
    #                        pending | queued | processing | complete | error | expired
    genome = Column(String(10), nullable=False, default="hg19")
    #                        hg19 | hg38
    variant_count = Column(Integer, nullable=False, default=0)
    annotated_count = Column(Integer, nullable=False, default=0)
    vcf_hash = Column(String(64), nullable=True)  # SHA-256 for Redis cache lookup
    error_message = Column(Text, nullable=True)
    celery_task_id = Column(String(36), nullable=True)
    queue_name = Column(String(10), nullable=True)  # 'small' | 'large'
    from_cache = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    queued_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=False, default=_expires)

    __table_args__ = (
        Index("idx_status", "status"),
        Index("idx_vcf_hash", "vcf_hash"),
        Index("idx_celery_task_id", "celery_task_id"),
        Index("idx_created_at", "created_at"),
        Index("idx_expires_at", "expires_at"),
    )

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "genome": self.genome,
            "vcf_hash": self.vcf_hash,
            "session_id": self.session_id,
            "queue_name": self.queue_name,
            "from_cache": self.from_cache,
            "variant_count": self.variant_count,
            "error_message": self.error_message,
            "celery_task_id": self.celery_task_id,
            "annotated_count": self.annotated_count,
            "queued_at": self.queued_at.isoformat() if self.queued_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    def __repr__(self):
        return f"<SessionStatus {self.session_id} [{self.status}]>"
