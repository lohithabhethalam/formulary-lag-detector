"""
ingest/openfda_client.py

Fetches drug approval and marketing data from two openFDA
endpoints. Both calls are wrapped with circuit breaker +
retry so transient API failures don't crash the pipeline.

Endpoints used:
- drug/drugsfda  → FDA approval date per ANDA application
- drug/ndc       → marketing_start_date (when pharmacies began selling)
"""

import requests
from datetime import datetime

from resilience.circuit_breaker import CircuitBreaker
from resilience.retry import retry

DRUGSFDA_URL = "https://api.fda.gov/drug/drugsfda.json"
NDC_URL = "https://api.fda.gov/drug/ndc.json"

# one circuit breaker shared across all openFDA calls —
# if the API is struggling, both endpoints back off together
openfda_circuit = CircuitBreaker(failure_threshold=3, cooldown_seconds=60)


@retry(max_attempts=3, base_delay=1.0, exceptions=(requests.RequestException,))
def _get(url: str, params: dict) -> dict:
    response = openfda_circuit.call(
        requests.get, url, params=params, timeout=30
    )
    response.raise_for_status()
    return response.json()


def fetch_anda_approvals(limit: int = 50) -> list:
    """
    Fetches recent ANDA (generic drug) approvals from openFDA.
    Returns list of dicts with application_number, drug_name,
    and approval_date (the original ORIG submission date).
    """
    data = _get(DRUGSFDA_URL, {
        "search": "application_number:ANDA*",
        "limit": limit
    })

    results = []
    for record in data.get("results", []):
        app_number = record.get("application_number")
        drug_name = (
            record["products"][0]["active_ingredients"][0]["name"]
            if record.get("products")
            else None
        )

        orig = next(
            (s for s in record.get("submissions", [])
             if s["submission_type"] == "ORIG"
             and s["submission_status"] == "AP"),
            None
        )
        approval_date = orig["submission_status_date"] if orig else None

        if app_number and drug_name and approval_date:
            results.append({
                "application_number": app_number,
                "drug_name": drug_name,
                "approval_date": approval_date,
            })

    return results


def fetch_marketing_start(application_number: str) -> str | None:
    """
    Fetches the marketing_start_date for a given ANDA application
    number from the openFDA NDC endpoint. Returns None if not found.
    """
    try:
        data = _get(NDC_URL, {
            "search": f"application_number:{application_number}",
            "limit": 1
        })
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise

    results = data.get("results", [])
    if not results:
        return None

    return results[0].get("marketing_start_date")