import sqlite3
from pathlib import Path

from .jobs import Job, JobFile, JobFileState, JobState


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

CREATE TABLE IF NOT EXISTS job_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    state TEXT NOT NULL,
    staged_path TEXT,
    status_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, source_path),
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
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
        conn.execute("PRAGMA foreign_keys = ON")
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

    def replace_job_files(
        self,
        job_id: int,
        files: list[tuple[Path, Path, JobFileState, str | None]],
    ) -> list[JobFile]:
        """Reconcile a fresh source scan without erasing processed child history.

        DISCOVERED and IGNORED rows are scan state and may be refreshed or
        removed when they disappear from the source tree. STAGED and
        NEEDS_ATTENTION rows are processing history and are preserved across
        later inventory scans.
        """
        if self.get_job(job_id) is None:
            raise ValueError(f"Unknown job id: {job_id}")

        with self._connect() as conn:
            existing_rows = conn.execute(
                "SELECT * FROM job_files WHERE job_id = ?",
                (job_id,),
            ).fetchall()
            existing_by_source = {row["source_path"]: row for row in existing_rows}
            seen_sources: set[str] = set()

            for source_path, relative_path, state, status_message in files:
                source_text = str(source_path)
                seen_sources.add(source_text)
                current = existing_by_source.get(source_text)

                if current is None:
                    conn.execute(
                        """
                        INSERT INTO job_files
                            (job_id, source_path, relative_path, state, status_message)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            job_id,
                            source_text,
                            str(relative_path),
                            state.value,
                            status_message,
                        ),
                    )
                    continue

                current_state = JobFileState(current["state"])
                if current_state in {JobFileState.STAGED, JobFileState.NEEDS_ATTENTION}:
                    continue

                conn.execute(
                    """
                    UPDATE job_files
                       SET relative_path = ?, state = ?, status_message = ?,
                           staged_path = NULL, updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?
                    """,
                    (
                        str(relative_path),
                        state.value,
                        status_message,
                        current["id"],
                    ),
                )

            for row in existing_rows:
                if row["source_path"] in seen_sources:
                    continue
                if JobFileState(row["state"]) in {
                    JobFileState.STAGED,
                    JobFileState.NEEDS_ATTENTION,
                }:
                    continue
                conn.execute("DELETE FROM job_files WHERE id = ?", (row["id"],))

            rows = conn.execute(
                "SELECT * FROM job_files WHERE job_id = ? ORDER BY relative_path",
                (job_id,),
            ).fetchall()
        return [self._row_to_job_file(row) for row in rows]

    def update_job_file(
        self,
        file_id: int,
        *,
        state: JobFileState | None = None,
        staged_path: Path | None = None,
        status_message: str | None = None,
    ) -> JobFile:
        with self._connect() as conn:
            current = conn.execute(
                "SELECT * FROM job_files WHERE id = ?", (file_id,)
            ).fetchone()
            if current is None:
                raise ValueError(f"Unknown job file id: {file_id}")

            next_state = state or JobFileState(current["state"])
            next_staged_path = (
                staged_path
                if staged_path is not None
                else (
                    None
                    if current["staged_path"] is None
                    else Path(current["staged_path"])
                )
            )
            next_message = (
                status_message
                if status_message is not None
                else current["status_message"]
            )

            conn.execute(
                """
                UPDATE job_files
                   SET state = ?, staged_path = ?, status_message = ?,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (
                    next_state.value,
                    None if next_staged_path is None else str(next_staged_path),
                    next_message,
                    file_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM job_files WHERE id = ?", (file_id,)
            ).fetchone()
        return self._row_to_job_file(row)

    def list_job_files(self, job_id: int) -> list[JobFile]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM job_files WHERE job_id = ? ORDER BY relative_path",
                (job_id,),
            ).fetchall()
        return [self._row_to_job_file(row) for row in rows]

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

    @staticmethod
    def _row_to_job_file(row: sqlite3.Row) -> JobFile:
        return JobFile(
            id=row["id"],
            job_id=row["job_id"],
            source_path=Path(row["source_path"]),
            relative_path=Path(row["relative_path"]),
            state=JobFileState(row["state"]),
            staged_path=None if row["staged_path"] is None else Path(row["staged_path"]),
            status_message=row["status_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
