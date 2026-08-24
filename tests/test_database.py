import sqlite3
from pathlib import Path

from filepup.database import JobStore
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
            """
        )

    JobStore(database_path)

    with sqlite3.connect(database_path) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }

    assert "staged_path" in columns
    assert "status_message" in columns
