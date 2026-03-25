import time
import httpx
from pathlib import Path
import fireducks.pandas as pd

# --- Configuration ---
MASTER_FILE = "files/data_713856.tsv"
BASE_URL = "http://10.10.6.80/nvpp/api/v1"  # Base URL without trailing slash
UPLOAD_URL = f"{BASE_URL}/upload"
STATUS_URL = f"{BASE_URL}/status/"  # session_id will be appended
REPETITIONS = 15
TEMP_DIR = Path("./benchmarking_files")
TEMP_DIR.mkdir(exist_ok=True)

# Define variant counts for benchmarking
file_counts = range(100, 600, 100)


def submit_jobs(client, master_df):
    """Phase 1: Generate and upload all files, capturing session IDs."""
    pending_jobs = []

    for count in file_counts:
        print(f"📦 Preparing {count} variants...", end=" ", flush=True)

        # Sample and Create File
        sample_df = master_df.sample(n=count)
        file_path = TEMP_DIR / f"data_{count}.tsv"
        sample_df.to_csv(file_path, sep="\t", index=False)

        for i in range(REPETITIONS):
            try:
                with file_path.open("rb") as fh:
                    files = {"file": (file_path.name, fh, "text/tab-separated-values")}
                    data = {"genome": "hg19", "file_format": "tsv"}

                    resp = client.post(
                        UPLOAD_URL, data=data, files=files, timeout=600.0
                    )
                    resp_data = resp.json()

                    if resp.status_code == 200:
                        pending_jobs.append(
                            {
                                "session_id": resp_data["session_id"],
                                "requested_variants": count,
                                "run_index": i,
                            }
                        )
                        print(".", end="", flush=True)
                    else:
                        print("E", end="", flush=True)
            except Exception as e:
                print("X", end="", flush=True)
        print(" Submitted.")

    return pending_jobs


def monitor_jobs(client, pending_jobs):
    """Phase 2: Poll status until all jobs are complete."""
    completed_results = []
    total_jobs = len(pending_jobs)

    print(f"\n🕵️  Monitoring {total_jobs} jobs...")

    while pending_jobs:
        still_pending = []
        for job in pending_jobs:
            try:
                # Poll the specific session ID
                url = f"{STATUS_URL}{job['session_id']}"
                resp = client.get(url, timeout=30.0)
                status_data = resp.json()

                if status_data.get("status") == "complete":
                    # Merge our request metadata with the server's response
                    result = {**job, **status_data}
                    completed_results.append(result)
                else:
                    still_pending.append(job)
            except Exception:
                still_pending.append(job)

        pending_jobs = still_pending
        percent = round(((total_jobs - len(pending_jobs)) / total_jobs) * 100, 1)
        print(
            f"⏳ Progress: {percent}% ({len(pending_jobs)} jobs remaining...)", end="\r"
        )

        if pending_jobs:
            time.sleep(300)  # Wait 5 minutes before next polling cycle

    print("\n✅ All jobs completed.")
    return completed_results


# --- Main Execution ---

# 1. Load Master File with Fireducks
master_df = pd.read_csv(MASTER_FILE, sep="\t")

with httpx.Client() as client:
    # Phase 1
    submitted_list = submit_jobs(client, master_df)

    pd.DataFrame(submitted_list).to_csv(
        "benchmark_submissions.tsv", sep="\t", index=False, header=True
    )

    # Phase 2
    final_data = monitor_jobs(client, submitted_list)

# 3. Export Results using Fireducks
if final_data:
    results_df = pd.DataFrame(final_data)

    # Add a calculated column for internal processing time if timestamps exist
    # Convert ISO strings to datetime objects
    results_df["started_at"] = pd.to_datetime(results_df["started_at"])
    results_df["completed_at"] = pd.to_datetime(results_df["completed_at"])
    results_df["internal_duration_sec"] = (
        results_df["completed_at"] - results_df["started_at"]
    ).dt.total_seconds()

    results_df.to_csv("benchmark_full_analysis.tsv", sep="\t", index=False)
    print("📊 Analysis exported to 'benchmark_full_analysis.tsv'")
