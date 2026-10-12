"""Record one pipeline run in OPS.PIPELINE_RUNS.

Reads dbt's target/run_results.json to count models and tests, and takes the
run's start time from the orchestrator. This is what makes your resume numbers
measured rather than estimated (see scripts/metrics.sql).

    python scripts/record_run.py --run-id <id> --started-at <iso> --orchestrator airflow
    python scripts/record_run.py --dry-run   # just print the summary
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def summarise(run_results_path: Path) -> dict:
    if not run_results_path.exists():  # dbt never ran (an earlier step failed)
        return {"dbt_models_run": 0, "dbt_tests_run": 0, "dbt_tests_passed": 0,
                "dbt_tests_warned": 0, "dbt_tests_failed": 0, "dbt_errors": 1}
    data = json.loads(run_results_path.read_text())
    models = tests = passed = warned = failed = errors = 0
    for r in data.get("results", []):
        uid = r.get("unique_id", "")
        status = r.get("status")
        if uid.startswith(("model.", "snapshot.", "seed.")):
            models += status == "success"
            errors += status == "error"
        elif uid.startswith("test."):
            tests += 1
            passed += status == "pass"
            warned += status == "warn"
            failed += status in ("fail", "error")
    return {"dbt_models_run": models, "dbt_tests_run": tests, "dbt_tests_passed": passed,
            "dbt_tests_warned": warned, "dbt_tests_failed": failed, "dbt_errors": errors}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default="manual")
    ap.add_argument("--started-at", help="ISO timestamp the pipeline started")
    ap.add_argument("--orchestrator", default="manual")
    ap.add_argument("--status", default="auto",
                    help="'auto' derives it from run_results.json")
    ap.add_argument("--run-results", default=str(ROOT / "dbt" / "target" / "run_results.json"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    summary = summarise(Path(args.run_results))
    errors = summary.pop("dbt_errors")
    if args.status == "auto":
        args.status = "failed" if (errors or summary["dbt_tests_failed"]) else "success"
    finished = datetime.now(UTC)
    started = datetime.fromisoformat(args.started_at) if args.started_at else finished
    duration = int((finished - started).total_seconds())
    row = {"run_id": args.run_id, "orchestrator": args.orchestrator, "started_at": started.isoformat(),
           "finished_at": finished.isoformat(), "duration_seconds": duration,
           "status": args.status, **summary}
    print(json.dumps(row, indent=2))
    if args.dry_run:
        return

    from ingestion.load_snowflake import _connect

    conn = _connect()
    try:
        conn.cursor().execute(
            """INSERT INTO OPS.PIPELINE_RUNS (run_id, orchestrator, started_at, finished_at,
                   duration_seconds, status, dbt_models_run, dbt_tests_run, dbt_tests_passed,
                   dbt_tests_warned, dbt_tests_failed)
               VALUES (%(run_id)s, %(orchestrator)s, %(started_at)s, %(finished_at)s,
                   %(duration_seconds)s, %(status)s, %(dbt_models_run)s, %(dbt_tests_run)s,
                   %(dbt_tests_passed)s, %(dbt_tests_warned)s, %(dbt_tests_failed)s)""",
            row,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
