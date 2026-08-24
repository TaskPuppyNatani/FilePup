import argparse
from pathlib import Path

from .config import FilePupConfig
from .database import JobStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="filepupctl")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Register a completed torrent path")
    ingest.add_argument("path", type=Path)

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

    if args.command == "jobs":
        for job in store.list_jobs():
            print(f"{job.id}\t{job.state.value}\t{job.source_path}")
