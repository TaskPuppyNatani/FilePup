import sqlite3
from pathlib import Path

from .jobs import Job, JobState


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    staged_path TEXT,
    status_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class JobStore:
    def __init__(self, database_path: Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate_jobs_table(conn)

    @staticmethod
    def _migrate_jobs_table(conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        if "staged_path" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN staged_path TEXT")
        if "status_message" not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN status_message TEXT")

    def ingest(self, source_path: Path) -> tuple[Job, bool]:
        source = str(Path(source_path).expanduser().resolve())
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM jobs WHERE source_path = ?", (source,)
            ).fetchone()
            if existing:
                return self._row_to_job(existing), False

            cursor = conn.execute(
                "INSERT INTO jobs (source_path, state) VALUES (?, ?)",
                (source, JobState.DISCOVERED.value),
            )
            row = conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
            return self._row_to_job(row), True

    def get_job(self, job_id: int) -> Job | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return None if row is None else self._row_to_job(row)

    def update_job(
        self,
        job_id: int,
        *,
        state: JobState | None = None,
        staged_path: Path | None = None,
        status_message: str | None = None,
    ) -> Job:
        current = self.get_job(job_id)
        if current is None:
            raise ValueError(f"Unknown job id: {job_id}")

        next_state = state or current.state
        next_staged_path = staged_path if staged_path is not None else current.staged_path
        next_message = status_message if status_message is not None else current.status_message

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                   SET state = ?, staged_path = ?, status_message = ?,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (
                    next_state.value,
                    None if next_staged_path is None else str(next_staged_path),
                    next_message,
                    job_id,
                ),
            )
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._row_to_job(row)

    def list_jobs(self) -> list[Job]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC").fetchall()
        return [self._row_to_job(row) for row in rows]

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        staged_path = row["staged_path"] if "staged_path" in row.keys() else None
        status_message = row["status_message"] if "status_message" in row.keys() else None
        return Job(
            id=row["id"],
            source_path=Path(row["source_path"]),
            state=JobState(row["state"]),
            staged_path=None if staged_path is None else Path(staged_path),
            status_message=status_message,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
