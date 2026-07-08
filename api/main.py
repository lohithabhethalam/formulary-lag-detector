"""
api/main.py

FastAPI service exposing formulary lag data from the
formulary-lag-detector pipeline. Queries DuckDB directly
on every request — results always reflect the most recent
pipeline run.
"""

from datetime import date

import duckdb
from fastapi import FastAPI, HTTPException

app = FastAPI(
    title="Formulary Lag Detector API",
    description="How long does it take generic drugs to reach pharmacies after FDA approval?",
    version="1.0.0",
)

DUCKDB_PATH = "data/formulary_lag.db"


def get_conn():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@app.get("/")
def root():
    return {
        "service": "Formulary Lag Detector API",
        "as_of": date.today().isoformat(),
    }


@app.get("/drugs/{application_number}/lag")
def get_drug_lag(application_number: str):
    """
    Returns formulary lag for a specific ANDA application number.
    Lag = weeks between FDA approval and first NADAC appearance.
    """
    conn = get_conn()
    result = conn.execute(
        """
        SELECT application_number, drug_name, approval_date,
               marketing_start_date, lag_days, lag_weeks, lag_category
        FROM fct_formulary_lag
        WHERE application_number = ?
        """,
        [application_number.upper()],
    ).fetchone()
    conn.close()

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No lag record found for '{application_number}'"
        )

    return {
        "application_number": result[0],
        "drug_name": result[1],
        "approval_date": result[2],
        "marketing_start_date": result[3],
        "lag_days": result[4],
        "lag_weeks": result[5],
        "lag_category": result[6],
        "as_of": date.today().isoformat(),
    }


@app.get("/drugs/avg-lag-by-category")
def get_avg_lag_by_category():
    """
    Returns average formulary lag grouped by category
    (fast/medium/slow). Same numbers as the Superset
    bar chart, served as JSON.
    """
    conn = get_conn()
    results = conn.execute(
        """
        SELECT lag_category,
               COUNT(*) as drug_count,
               ROUND(AVG(lag_weeks), 1) as avg_lag_weeks,
               ROUND(MIN(lag_weeks), 1) as min_lag_weeks,
               ROUND(MAX(lag_weeks), 1) as max_lag_weeks
        FROM fct_formulary_lag
        GROUP BY lag_category
        ORDER BY avg_lag_weeks
        """
    ).fetchall()
    conn.close()

    return {
        "as_of": date.today().isoformat(),
        "categories": [
            {
                "lag_category": r[0],
                "drug_count": r[1],
                "avg_lag_weeks": r[2],
                "min_lag_weeks": r[3],
                "max_lag_weeks": r[4],
            }
            for r in results
        ],
    }


@app.get("/drugs/slow-to-market")
def get_slow_to_market(min_weeks: float = 78.0):
    """
    Returns drugs that took longer than min_weeks to reach
    pharmacies after FDA approval. Default threshold is 78
    weeks (18 months) — the boundary between medium and slow
    in our lag_category classification.
    """
    conn = get_conn()
    results = conn.execute(
        """
        SELECT application_number, drug_name, approval_date,
               marketing_start_date, lag_weeks, lag_category
        FROM fct_formulary_lag
        WHERE lag_weeks >= ?
        ORDER BY lag_weeks DESC
        """,
        [min_weeks],
    ).fetchall()
    conn.close()

    return {
        "as_of": date.today().isoformat(),
        "min_weeks_threshold": min_weeks,
        "drug_count": len(results),
        "drugs": [
            {
                "application_number": r[0],
                "drug_name": r[1],
                "approval_date": r[2],
                "marketing_start_date": r[3],
                "lag_weeks": r[4],
                "lag_category": r[5],
            }
            for r in results
        ],
    }