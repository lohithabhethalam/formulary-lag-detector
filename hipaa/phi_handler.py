"""
hipaa/phi_handler.py

Demonstrates 5 HIPAA Safe Harbor de-identification and
handling patterns on synthetic member data. None of these
records contain real PHI — Faker-generated data only.

Pattern 1: Masking at ingestion
Pattern 2: Encryption at rest (key derivation pattern)
Pattern 3: Audit logging
Pattern 4: Data minimization
Pattern 5: Safe aggregation (11-member minimum)
"""

import hashlib
import hmac
import json
import os
from collections import defaultdict
from datetime import datetime, date


# ── Pattern 3: Audit log ─────────────────────────────────────────────────────

_audit_log = []

def _audit(requester: str, action: str, record_count: int, purpose: str):
    """Logs every PHI access with who, what, when, and why."""
    _audit_log.append({
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "requester": requester,
        "action": action,
        "record_count": record_count,
        "purpose": purpose,
    })

def get_audit_log() -> list:
    return _audit_log


# ── Pattern 1: Masking at ingestion ──────────────────────────────────────────

def _mask_name(name: str) -> str:
    """Sarah Johnson → S.J."""
    parts = name.strip().split()
    return ".".join(p[0] for p in parts if p) + "."


def _mask_dob(dob_str: str) -> str:
    """1978-03-15 → 40-49 (age group in 10-year bands)"""
    try:
        dob = date.fromisoformat(dob_str)
        age = (date.today() - dob).days // 365
        decade = (age // 10) * 10
        return f"{decade}-{decade + 9}"
    except ValueError:
        return "unknown"


def _mask_zip(zip_code: str) -> str:
    """94102 → 941** (first 3 digits only, per HIPAA Safe Harbor)"""
    return zip_code[:3] + "**" if len(zip_code) >= 3 else "***"


def mask_record(record: dict) -> dict:
    """
    Masks all PHI fields before the record touches storage.
    Only member_id, masked fields, and analytical fields are kept.
    """
    return {
        "member_id": record["member_id"],
        "name_masked": _mask_name(record["name"]),
        "age_group": _mask_dob(record["dob"]),
        "zip_masked": _mask_zip(record["zip_code"]),
        "drug_ndc": record["drug_ndc"],
        "fill_date": record["fill_date"],
        "days_supply": record["days_supply"],
    }


# ── Pattern 2: Encryption at rest (key derivation pattern) ───────────────────

def derive_encryption_key(secret: str, salt: str) -> str:
    """
    Derives a deterministic AES-256 key from a secret and salt
    using HMAC-SHA256. In production, the secret comes from a
    secrets manager (AWS Secrets Manager, HashiCorp Vault) —
    never hardcoded. Shown here as a pattern, not a full
    AES implementation.
    """
    key = hmac.new(
        secret.encode(),
        salt.encode(),
        hashlib.sha256
    ).hexdigest()
    return key


# ── Pattern 4: Data minimization ─────────────────────────────────────────────

ALLOWED_FIELDS = {
    "member_id", "age_group", "zip_masked",
    "drug_ndc", "fill_date", "days_supply"
}

def minimize(record: dict) -> dict:
    """
    Strips any field not needed for the downstream analysis.
    Even masked fields like name_masked are dropped unless
    explicitly needed.
    """
    return {k: v for k, v in record.items() if k in ALLOWED_FIELDS}


# ── Pattern 5: Safe aggregation ──────────────────────────────────────────────

SAFE_HARBOR_MINIMUM = 11  # HIPAA Safe Harbor: never report groups < 11

def safe_aggregate(records: list, group_by: str) -> dict:
    """
    Groups records by a field and suppresses any group with
    fewer than 11 members — the HIPAA Safe Harbor minimum.
    Returns only groups large enough to be safely reported.
    """
    groups = defaultdict(list)
    for r in records:
        groups[r.get(group_by, "unknown")].append(r)

    result = {}
    suppressed = []
    for key, members in groups.items():
        if len(members) >= SAFE_HARBOR_MINIMUM:
            result[key] = {
                "count": len(members),
                "avg_days_supply": round(
                    sum(m["days_supply"] for m in members) / len(members), 1
                )
            }
        else:
            suppressed.append(key)

    if suppressed:
        result["_suppressed"] = {
            "reason": f"Groups with fewer than {SAFE_HARBOR_MINIMUM} members suppressed per HIPAA Safe Harbor",
            "suppressed_groups": suppressed
        }

    return result


# ── Full pipeline ─────────────────────────────────────────────────────────────

def process_members(
    raw_records: list,
    requester: str = "pipeline",
    purpose: str = "formulary_lag_analysis"
) -> list:
    """
    Runs all 5 HIPAA patterns on a batch of raw member records:
    1. Mask PHI at ingestion
    2. (Key derivation shown separately)
    3. Audit log the access
    4. Minimize to only needed fields
    5. (Safe aggregation applied separately on grouped queries)
    """
    _audit(requester, "read_raw_phi", len(raw_records), purpose)

    masked = [mask_record(r) for r in raw_records]
    minimized = [minimize(r) for r in masked]

    _audit(requester, "write_masked_records", len(minimized), purpose)

    return minimized