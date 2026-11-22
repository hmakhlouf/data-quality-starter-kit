"""Run data quality rules, store every result, and write an issue log.

Usage:
    python dq/run_checks.py                       # demo: build the pipeline in DuckDB, run all rules
    python dq/run_checks.py --fail-on-critical    # exit code 1 if a critical rule fails (use in CI / Airflow)
    python dq/run_checks.py --db my.duckdb --no-demo --rules rules/rules.yml

The rules are plain SQL in rules/rules.yml. To run them on Postgres, Snowflake, BigQuery,
SQL Server etc., replace `connect()` below with that database's DB-API connection and
adapt any engine-specific SQL functions in the rules.
"""
import argparse
import csv
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parent.parent
MAX_EXAMPLES = 5

RESULTS_DDL = """
CREATE SCHEMA IF NOT EXISTS dq;
CREATE TABLE IF NOT EXISTS dq.dq_run (
    run_id VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP, status VARCHAR,
    rules_total INTEGER, rules_failed INTEGER, critical_failed INTEGER);
CREATE TABLE IF NOT EXISTS dq.dq_result (
    run_id VARCHAR, rule_id VARCHAR, rule_name VARCHAR, dimension VARCHAR, stage VARCHAR,
    severity VARCHAR, owner VARCHAR, status VARCHAR, failed_rows INTEGER,
    examples VARCHAR, checked_at TIMESTAMP);
"""


def connect(db_path):
    return duckdb.connect(db_path)


def run_rule(con, rule):
    """Return (failed_rows, examples). Any SQL error counts as a failure of the rule."""
    try:
        sql = rule["sql"].strip().rstrip(";")
        rows = con.execute(f"SELECT * FROM ({sql}) AS failing ORDER BY 1").fetchall()
    except Exception as exc:  # noqa: BLE001 - a broken rule must be visible, not skipped
        return -1, [f"RULE ERROR: {exc}".splitlines()[0][:200]]
    examples = [str(r[0]) for r in rows[:MAX_EXAMPLES]]
    return len(rows), examples


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(ROOT / "output" / "demo.duckdb"))
    ap.add_argument("--rules", default=str(ROOT / "rules" / "rules.yml"))
    ap.add_argument("--no-demo", action="store_true", help="don't rebuild the demo pipeline")
    ap.add_argument("--fail-on-critical", action="store_true", help="exit 1 when a critical rule fails")
    args = ap.parse_args()

    out = ROOT / "output"
    out.mkdir(exist_ok=True)
    con = connect(args.db)

    if not args.no_demo:
        import os
        os.chdir(ROOT)  # pipeline.sql reads demo/data/... relative to the repo root
        con.execute((ROOT / "demo" / "pipeline.sql").read_text())
        # Export the target so the Great Expectations example can read it
        con.execute(f"COPY tgt_program_records TO '{out / 'tgt_program_records.csv'}' (HEADER)")

    con.execute(RESULTS_DDL)
    rules = yaml.safe_load(Path(args.rules).read_text())["rules"]
    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc).replace(tzinfo=None)

    results = []
    for rule in rules:
        failed, examples = run_rule(con, rule)
        status = "PASS" if failed == 0 else ("ERROR" if failed < 0 else "FAIL")
        results.append({
            "rule_id": rule["id"], "rule_name": rule["name"], "dimension": rule["dimension"],
            "stage": rule["stage"], "severity": rule["severity"], "owner": rule["owner"],
            "status": status, "failed_rows": max(failed, 0), "examples": "; ".join(examples),
        })

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.executemany(
        "INSERT INTO dq.dq_result VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(run_id, r["rule_id"], r["rule_name"], r["dimension"], r["stage"], r["severity"],
          r["owner"], r["status"], r["failed_rows"], r["examples"], now) for r in results],
    )
    failed = [r for r in results if r["status"] != "PASS"]
    critical = [r for r in failed if r["severity"] == "critical"]
    run_status = "STOP" if critical else ("WARN" if failed else "PASS")
    con.execute("INSERT INTO dq.dq_run VALUES (?, ?, ?, ?, ?, ?, ?)",
                (run_id, started, now, run_status, len(results), len(failed), len(critical)))

    # Issue log: one row per failed rule, ready to paste into Excel / Sheets / Jira
    with open(out / "issue_log.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["issue_id", "found", "stage", "rule", "dimension", "severity",
                    "rows", "examples", "owner", "root_cause", "status"])
        for i, r in enumerate(failed, 1):
            w.writerow([f"DQ-{i:03d}", now.date(), r["stage"], f'{r["rule_id"]} {r["rule_name"]}',
                        r["dimension"], r["severity"], r["failed_rows"], r["examples"],
                        r["owner"], "", "Open"])

    # Markdown report
    icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "⚠️"}
    lines = [
        f"# Data quality report: run `{run_id}`",
        "",
        f"**Result: {run_status}**: {len(results)} rules, {len(failed)} failed, "
        f"{len(critical)} critical. Checked {now:%Y-%m-%d %H:%M} UTC.",
        "",
        "| | Rule | Stage | Severity | Rows | Examples | Owner |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in results:
        lines.append(f'| {icon[r["status"]]} | {r["rule_id"]} {r["rule_name"]} | {r["stage"]} | '
                     f'{r["severity"]} | {r["failed_rows"]} | {r["examples"]} | {r["owner"]} |')
    (out / "report.md").write_text("\n".join(lines) + "\n")

    # Console summary
    print(f"\nData quality run {run_id}: {run_status}\n")
    for r in results:
        print(f'  {r["status"]:<5} {r["severity"]:<8} {r["rule_id"]:<8} {r["rule_name"]:<55} rows={r["failed_rows"]}')
    print(f"\n  {len(failed)} of {len(results)} rules failed ({len(critical)} critical).")
    print(f"  Issue log: {out / 'issue_log.csv'}\n  Report:    {out / 'report.md'}\n")

    if args.fail_on_critical and critical:
        sys.exit(1)


if __name__ == "__main__":
    main()
