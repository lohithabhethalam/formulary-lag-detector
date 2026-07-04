"""
hipaa/synthetic_members.py

Generates synthetic member records using Faker.
These are fake but realistic PHI records used only to
demonstrate HIPAA handling patterns — no real patient
data is used or stored anywhere in this project.
"""

import random
from faker import Faker

fake = Faker()

# Sample ANDA application numbers from our pipeline
SAMPLE_NDCS = [
    "ANDA076367",  # AMCINONIDE
    "ANDA077824",  # RANITIDINE HYDROCHLORIDE
    "ANDA091316",  # FONDAPARINUX SODIUM
    "ANDA090548",  # ATORVASTATIN CALCIUM
    "ANDA078284",  # SUMATRIPTAN
]


def generate_members(n: int = 100) -> list:
    """
    Generates n synthetic member records with realistic
    but entirely fake PHI. Safe to use for demonstrations
    and testing — contains no real patient data.
    """
    members = []
    for i in range(n):
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
        members.append({
            "member_id": f"M-{1000 + i}",
            "name": fake.name(),
            "dob": dob.strftime("%Y-%m-%d"),
            "zip_code": fake.zipcode(),
            "drug_ndc": random.choice(SAMPLE_NDCS),
            "fill_date": fake.date_between(
                start_date="-2y", end_date="today"
            ).strftime("%Y-%m-%d"),
            "days_supply": random.choice([30, 60, 90]),
        })
    return members


if __name__ == "__main__":
    members = generate_members(10)
    for m in members:
        print(m)