"""
ingest/fetch_lag_batch.py

Pulls a batch of ANDA approvals from openFDA, joins with
marketing_start_date, and calculates formulary lag per drug.
Saves results to data/raw/lag_batch.json for downstream
PySpark processing.

Skips drugs where:
- marketing_start_date is missing (no NDC record)
- lag is negative (data error — marketed before approval)
- lag exceeds 3650 days (10 years — likely a new NDC for
  an old drug, not a genuine first-to-market lag)
"""

import json
from datetime import datetime
from pathlib import Path

from ingest.openfda_client import fetch_anda_approvals, fetch_marketing_start

MAX_LAG_DAYS = 3650  # 10 years — filters out stale NDC registrations


def calculate_lag(approval_date: str, marketing_start: str) -> dict | None:
    """
    Returns lag in days and weeks, or None if the dates
    produce an invalid result.
    """
    try:
        approval_dt = datetime.strptime(approval_date, "%Y%m%d")
        marketing_dt = datetime.strptime(marketing_start, "%Y%m%d")
        lag_days = (marketing_dt - approval_dt).days

        if lag_days < 0 or lag_days > MAX_LAG_DAYS:
            return None

        return {
            "lag_days": lag_days,
            "lag_weeks": round(lag_days / 7, 1),
        }
    except (ValueError, TypeError):
        return None


def fetch_lag_batch(limit: int = 50) -> list:
    """
    Fetches a batch of ANDA approvals, calculates lag for each,
    and returns only drugs with a valid, plausible lag value.
    """
    approvals = fetch_anda_approvals(limit=limit)
    print(f"Fetched {len(approvals)} ANDA approvals")

    results = []
    skipped = 0

    for drug in approvals:
        marketing_start = fetch_marketing_start(drug["application_number"])

        if not marketing_start:
            skipped += 1
            continue

        lag = calculate_lag(drug["approval_date"], marketing_start)

        if not lag:
            skipped += 1
            continue

        results.append({
            "application_number": drug["application_number"],
            "drug_name": drug["drug_name"],
            "approval_date": drug["approval_date"],
            "marketing_start_date": marketing_start,
            **lag,
        })

    print(f"Valid lag records: {len(results)} | Skipped: {skipped}")
    return results


if __name__ == "__main__":
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    results = fetch_lag_batch(limit=50)

    output_path = "data/raw/lag_batch.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Saved {len(results)} records to {output_path}")

    # Quick summary
    if results:
        lags = [r["lag_weeks"] for r in results]
        print(f"Lag range: {min(lags):.1f} to {max(lags):.1f} weeks")
        print(f"Average lag: {sum(lags)/len(lags):.1f} weeks")