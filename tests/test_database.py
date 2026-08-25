import sqlite3
from pathlib import Path

from filepup.database import SCHEMA_VERSION, JobStore
from filepup.jobs import JobState


def test_ingest_is_idempotent(tmp_path: Path) -> None:
    store = JobStore(tmp_path / "filepup.db")
    source = tmp_path / "Bluey.S03E12.mkv"

    first, first_created = store.ingest(source)
    second, second_created = store.ingest(source)

    assert first_created is True
    assert second_created is False
    assert first.id == second.id
    assert first.state is JobState.DISCOVERED


def test_existing_database_is_migrated_in_place(tmp_path: Path) -> None:
    database_path = tmp_path / "filepup.db"
    with sqlite3.connect(database_path) as conn:
        conn.executescript(
            """
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO jobs (source_path, state)
            VALUES ('/tmp/legacy-job', 'DISCOVERED');
            """
        )

    store = JobStore(database_path)

    with sqlite3.connect(database_path) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        version = conn.execute("PRAGMA user_version").fetchone()[0]

    assert "staged_path" in columns
    assert "status_message" in columns
    assert {"move_operations", "job_claims"} <= tables
    assert version == SCHEMA_VERSION
    legacy = store.get_job(1)
    assert legacy is not None
    assert legacy.source_path == Path("/tmp/legacy-job")
    assert legacy.state is JobState.DISCOVERED
