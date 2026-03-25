# ══════════════════════════════════════════════════════════════════
# Thresholds — align with tasks.py strategy switch
# ══════════════════════════════════════════════════════════════════

SMALL_VARIANT_THRESHOLD = 1_000  # WHERE IN  vs JOIN boundary
LARGE_VARIANT_THRESHOLD = 200_000  # single JOIN vs chunked JOIN boundary

QUEUE_SMALL_CUTOFF = 10_000  # queue routing boundary
MAX_VARIANTS = 100_000  # normalisation ceiling for priority scale

# Queue names — must match Celery worker -Q arguments
QUEUE_SMALL = "variants.small"
QUEUE_LARGE = "variants.large"


# ══════════════════════════════════════════════════════════════════
# Priority
# ══════════════════════════════════════════════════════════════════


def _variant_priority(variant_count: int) -> int:
    """
    Maps variant_count to a Redis Celery priority.

    Redis priority scale:
        0 = highest priority  (runs first)
        9 = lowest priority   (runs last)

    Fewer variants → smaller number → executes before larger jobs.

    Normalised against MAX_VARIANTS so the full 0–9 scale is used:
        0        variants → priority 0  (immediate)
        10,000   variants → priority 0
        50,000   variants → priority 4
        100,000  variants → priority 9
        >100,000 variants → priority 9  (capped)

    Examples:
        >>> _variant_priority(500)
        0
        >>> _variant_priority(50_000)
        4
        >>> _variant_priority(100_000)
        9
        >>> _variant_priority(250_000)   # above ceiling — capped at 9
        9
    """
    ratio = min(variant_count / MAX_VARIANTS, 1.0)
    return int(ratio * 9)


# ══════════════════════════════════════════════════════════════════
# Queue routing
# ══════════════════════════════════════════════════════════════════


def _variant_queue(variant_count: int) -> str:
    """
    Routes a job to a dedicated worker pool based on variant count.

    variants.small  → worker pool with more slots, fast turnaround
                      handles WHERE IN jobs and small JOIN jobs
    variants.large  → worker pool with fewer slots, long-running jobs
                      handles large JOIN and chunked JOIN jobs

    The cutoff (QUEUE_SMALL_CUTOFF=10,000) sits between the WHERE IN
    threshold (1,000) and the chunked threshold (200,000) so that:
      - Tiny jobs (< 1k)    → small queue, WHERE IN path
      - Medium jobs (1k–10k)→ small queue, JOIN path
      - Large jobs (> 10k)  → large queue, JOIN or chunked path

    Examples:
        >>> _variant_queue(500)
        'variants.small'
        >>> _variant_queue(9_999)
        'variants.small'
        >>> _variant_queue(10_000)
        'variants.large'
        >>> _variant_queue(300_000)
        'variants.large'
    """
    return QUEUE_SMALL if variant_count < QUEUE_SMALL_CUTOFF else QUEUE_LARGE


# ══════════════════════════════════════════════════════════════════
# Combined helper
# ══════════════════════════════════════════════════════════════════


def get_task_routing(variant_count: int) -> dict:
    """
    Returns both priority and queue in one call.
    Use at submission time in upload_variants.

    Returns:
        {
            "priority":  int,   # 0–9
            "queue":     str,   # "variants.small" | "variants.large"
        }

    Example:
        routing = get_task_routing(variant_count)
        materialise_annotations.apply_async(
            args=[session_id, variant_count],
            **routing,
        )
    """
    return {
        "priority": _variant_priority(variant_count),
        "queue": _variant_queue(variant_count),
    }


# ══════════════════════════════════════════════════════════════════
# Human-readable label (useful for logging / status records)
# ══════════════════════════════════════════════════════════════════


def _variant_strategy_label(variant_count: int) -> str:
    """
    Returns the annotation strategy that tasks.py will use for this count.
    Purely informational — used in logs and session_status.queue_name.

    Examples:
        >>> _variant_strategy_label(500)
        'where_in'
        >>> _variant_strategy_label(5_000)
        'single_join'
        >>> _variant_strategy_label(250_000)
        'chunked_join'
    """
    if variant_count < SMALL_VARIANT_THRESHOLD:
        return "where_in"
    elif variant_count <= LARGE_VARIANT_THRESHOLD:
        return "single_join"
    else:
        return "chunked_join"
