from celery_app import celery_app
from celery.utils.log import get_task_logger
from app.api.job_status import get_job_status, create_or_update_job_status
from app.session import get_local_background_client
from app.api.queue import QUEUE_SMALL, QUEUE_LARGE

logger = get_task_logger(__name__)

# ══════════════════════════════════════════════════════════════════
# Tuning
# ══════════════════════════════════════════════════════════════════

AGING_INTERVAL_SECONDS = 60  # how often the aging task runs via Celery Beat
STALE_THRESHOLD_SECONDS = 120  # minimum wait before first boost
AGING_BOOST_PER_INTERVAL = 2  # priority reduced by this much each interval
MIN_PRIORITY = 0  # Redis floor — cannot go higher than this


# ══════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════


def _get_all_queued_sessions() -> list[dict]:
    """
    Scans local ClickHouse session_status for all jobs currently
    in status='queued' that have a celery_task_id and enqueued timestamp.
    """
    client = get_local_background_client()
    try:
        result = client.query(
            """
            SELECT
                session_id,
                celery_task_id,
                current_priority,
                queue_name,
                variant_count,
                queued_at
            FROM nc_spark.session_status FINAL
            WHERE status = 'queued'
              AND celery_task_id IS NOT NULL
              AND queued_at     IS NOT NULL
            ORDER BY queued_at ASC
            """
        )
        return [dict(zip(result.column_names, row)) for row in result.result_rows]
    finally:
        client.close()


def _seconds_waiting(queued_at) -> float:
    """Returns how many seconds a job has been in the queue."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    # queued_at may arrive as a naive datetime from ClickHouse — normalise it
    if queued_at.tzinfo is None:
        queued_at = queued_at.replace(tzinfo=timezone.utc)
    return (now - queued_at).total_seconds()


def _compute_max_wait(initial_priority: int) -> float:
    """
    Returns the theoretical maximum wait time in seconds for a job
    starting at initial_priority, given current aging config.

    Formula:
        max_wait = STALE_THRESHOLD + (initial_priority / BOOST_PER_INTERVAL) × INTERVAL
    """
    boosts_needed = initial_priority / AGING_BOOST_PER_INTERVAL
    return STALE_THRESHOLD_SECONDS + boosts_needed * AGING_INTERVAL_SECONDS


# ══════════════════════════════════════════════════════════════════
# Aging Task (runs every AGING_INTERVAL_SECONDS via Celery Beat)
# ══════════════════════════════════════════════════════════════════


@celery_app.task(bind=True)
def boost_stale_tasks(self):
    """
    Scans all queued jobs and boosts the priority of any that have
    been waiting longer than STALE_THRESHOLD_SECONDS.

    For each eligible job:
      1. Revoke the existing Celery task (non-terminating)
      2. Re-queue under a lower priority number (= higher execution priority)
      3. Update session_status with new celery_task_id + current_priority
    """
    from app.api.annotation import materialise_annotations

    queued_jobs = _get_all_queued_sessions()
    if not queued_jobs:
        logger.debug("[aging] No queued jobs found.")
        return

    logger.info(f"[aging] Scanning {len(queued_jobs)} queued jobs")
    boosted = 0

    for job in queued_jobs:
        session_id = job["session_id"]
        celery_task_id = job["celery_task_id"]
        current_priority = job["current_priority"]
        queue_name = job["queue_name"]
        variant_count = job["variant_count"]
        queued_at = job["queued_at"]

        # ── Rule 1: skip if already at max priority ───────────────────────────
        if current_priority <= MIN_PRIORITY:
            logger.debug(f"[aging] {session_id} already at max priority — skip")
            continue

        # ── Rule 2: skip if not yet stale ─────────────────────────────────────
        wait_seconds = _seconds_waiting(queued_at)
        if wait_seconds < STALE_THRESHOLD_SECONDS:
            logger.debug(
                f"[aging] {session_id} waited {wait_seconds:.0f}s "
                f"< threshold {STALE_THRESHOLD_SECONDS}s — skip"
            )
            continue

        # ── Rule 7: skip if task already started (revoke race guard) ──────────
        fresh_record = get_job_status(session_id)
        if fresh_record and fresh_record.status in ("processing", "complete", "error"):
            logger.info(f"[aging] {session_id} already {fresh_record.status} — skip")
            continue

        # ── Rule 3 + 4: compute new priority, floor at MIN_PRIORITY ──────────
        new_priority = max(MIN_PRIORITY, current_priority - AGING_BOOST_PER_INTERVAL)

        # ── Rule 5: preserve original queue ───────────────────────────────────
        queue = queue_name or QUEUE_LARGE

        logger.info(
            f"[aging] Boosting {session_id} | "
            f"waited={wait_seconds:.0f}s | "
            f"priority {current_priority} → {new_priority} | "
            f"queue={queue} | variants={variant_count} | "
            f"max_wait={_compute_max_wait(current_priority):.0f}s"
        )

        # ── Rule 6: revoke old task (non-terminating) ─────────────────────────
        try:
            celery_app.control.revoke(celery_task_id, terminate=False)
        except Exception as e:
            logger.warning(f"[aging] Could not revoke {celery_task_id}: {e}")

        # ── Re-queue at boosted priority ──────────────────────────────────────
        new_result = materialise_annotations.apply_async(
            args=[session_id, variant_count],
            priority=new_priority,
            queue=queue,
        )

        # ── Persist updated metadata ──────────────────────────────────────────
        create_or_update_job_status(
            session_id,
            celery_task_id=new_result.id,
            current_priority=new_priority,
        )

        boosted += 1

    logger.info(f"[aging] Cycle complete — boosted {boosted}/{len(queued_jobs)} jobs")
