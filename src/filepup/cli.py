import argparse
from pathlib import Path

from .config import FilePupConfig
from .database import JobStore
from .intake import IntakeEngine
from .inventory import InventoryEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="filepupctl")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Register a completed torrent path")
    ingest.add_argument("path", type=Path)

    inventory = sub.add_parser("inventory", help="Inventory files belonging to a job")
    inventory.add_argument("job_id", type=int)

    stage = sub.add_parser("stage", help="Move a discovered job safely into Staging")
    stage.add_argument("job_id", type=int)

    retry = sub.add_parser(
        "retry",
        help="Retry unresolved files for a job that needs attention",
    )
    retry.add_argument("job_id", type=int)

    recover = sub.add_parser(
        "recover",
        help="Reconcile interrupted filesystem moves for a job",
    )
    recover.add_argument("job_id", type=int)

    sub.add_parser("jobs", help="List known jobs")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = FilePupConfig()
    store = JobStore(config.database_path)

    if args.command == "ingest":
        job, created = store.ingest(args.path)
        verb = "created" if created else "already known"
        print(f"Job {job.id} {verb}: {job.state.value} {job.source_path}")
        return

    if args.command == "inventory":
        result = InventoryEngine(store).inventory(args.job_id)
        print(
            f"Job {args.job_id}: {result.supported_count} supported, "
            f"{result.ignored_count} ignored"
        )
        for file in result.files:
            print(f"{file.state.value}\t{file.relative_path}\t{file.status_message}")
        return

    if args.command == "stage":
        result = IntakeEngine(config, store).stage(args.job_id)
        print(f"Job {result.job.id}: {result.job.state.value} - {result.message}")
        return

    if args.command == "retry":
        result = IntakeEngine(config, store).retry(args.job_id)
        print(f"Job {result.job.id}: {result.job.state.value} - {result.message}")
        return

    if args.command == "recover":
        result = IntakeEngine(config, store).recover(args.job_id)
        print(f"Job {result.job.id}: {result.job.state.value} - {result.message}")
        return

    if args.command == "jobs":
        for job in store.list_jobs():
            details = f"\t{job.status_message}" if job.status_message else ""
            print(f"{job.id}\t{job.state.value}\t{job.source_path}{details}")
