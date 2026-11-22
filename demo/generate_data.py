"""Generate the demo source export: a raw, all-text CSV like a Quickbase/Airtable/Excel export.

Deterministic (fixed seed) so everyone gets the same data and the same findings.
Some problems are planted on purpose so the checks have something to find:
  - free-text dates that don't parse            (data entry problem)
  - category spellings missing from the mapping (ETL mapping problem)
  - blank program IDs                           (data entry problem)
  - program IDs not in the program list         (orphans)
  - amounts with "$" and "," as text            (handled by the pipeline)
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent / "data"
OUT.mkdir(exist_ok=True)

programs = [
    ("P01", "Youth Services"), ("P02", "Housing Support"), ("P03", "Job Training"),
    ("P04", "Health Outreach"), ("P05", "Food Assistance"),
]
categories = ["Youth Services", "Housing Support", "Job Training", "Health Outreach", "Food Assistance"]
statuses = ["Open", "Closed", "Pending"]

with open(OUT / "programs.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["program_id", "program_name"])
    w.writerows(programs)

rows = []
start = date(2025, 1, 1)
for i in range(1, 1001):
    pid, cat = random.choice(programs)
    d = start + timedelta(days=random.randint(0, 600))
    amount = round(random.uniform(50, 5000), 2)
    rows.append({
        "record_id": f"R{i:05d}",
        "program_id": pid,
        "category": cat,
        "status": random.choice(statuses),
        "start_date": d.strftime("%m/%d/%Y"),
        "amount": f"${amount:,.2f}" if random.random() < 0.3 else f"{amount}",
    })

# Planted problems
for r in random.sample(rows, 12):
    r["start_date"] = random.choice(["13/31/2025", "TBD", "2025-02-30", "next week"])
for r in random.sample(rows, 9):
    r["category"] = random.choice(["Youth  Svc", "housing support ", "Job-Training"])
for r in random.sample(rows, 6):
    r["program_id"] = ""
for r in random.sample(rows, 4):
    r["program_id"] = "P99"

with open(OUT / "source_export.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

print(f"Wrote {len(rows)} records to {OUT / 'source_export.csv'}")
