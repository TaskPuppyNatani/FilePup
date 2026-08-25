import errno
import fcntl
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from .jobs import (
    Job,
    JobClaim,
    JobFile,
    JobFileState,
    JobState,
    MoveOperation,
    MoveOperationPhase,
    MoveOperationType,
)


SCHEMA_VERSION = 2
CLAIM_BUSY_TIMEOUT_MS = 5000

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

CREATE TABLE IF NOT EXISTS move_operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    job_file_id INTEGER,
    operation_type TEXT NOT NULL,
    phase TEXT NOT NULL,
    source_path TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    source_size INTEGER NOT NULL,
    source_sha256 TEXT NOT NULL,
    prior_file_state TEXT,
    owner_token TEXT NOT NULL,
    claim_fence INTEGER NOT NULL,
    status_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(job_id) REFERENCES jobs(id),
    FOREIGN KEY(job_file_id) REFERENCES job_files(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_open_move_per_job_file
    ON move_operations(job_id, COALESCE(job_file_id, -1))
    WHERE phase IN ('PREPARED', 'RENAMED');

CREATE INDEX IF NOT EXISTS move_operations_job_idx
    ON move_operations(job_id, id);

CREATE TABLE IF NOT EXISTS job_claims (
    job_id INTEGER PRIMARY KEY,
    owner_token TEXT NOT NULL,
    fence INTEGER NOT NULL,
    claimed_at REAL NOT NULL,
    lease_until REAL NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
"""


class JobClaimError(RuntimeError):
    """Raised when a worker no longer owns its job claim."""


class JobBusyError(RuntimeError):
    """Raised when another process currently owns a job lock."""


class JobStore:
    def __init__(self, database_path: Path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {CLAIM_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate_jobs_table(conn)
            current_version = conn.execute("PRAGMA user_version").fetchone()[0]
            if current_version < SCHEMA_VERSION:
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    @contextmanager
    def job_lock(self, job_id: int):
        """Hold an OS lock for one job without serializing unrelated jobs."""
        lock_dir = self.database_path.parent / "locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = lock_dir / f"job-{job_id}.lock"
        handle = lock_path.open("a+")
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    raise JobBusyError(f"Job {job_id} is already being processed") from exc
                raise
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

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
            cursor = conn.execute(
                """
                INSERT INTO jobs (source_path, state)
                VALUES (?, ?)
                ON CONFLICT(source_path) DO NOTHING
                """,
                (source, JobState.DISCOVERED.value),
            )
            row = conn.execute(
                "SELECT * FROM jobs WHERE source_path = ?", (source,)
            ).fetchone()
            return self._row_to_job(row), cursor.rowcount == 1

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
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise ValueError(f"Unknown job id: {job_id}")
            updated = self._update_job_row(
                conn,
                row,
                job_id=job_id,
                state=state,
                staged_path=staged_path,
                status_message=status_message,
            )
        return self._row_to_job(updated)

    def update_job_claimed(
        self,
        job_id: int,
        *,
        owner_token: str,
        claim_fence: int,
        state: JobState | None = None,
        staged_path: Path | None = None,
        status_message: str | None = None,
    ) -> Job:
        with self._connect() as conn:
            self._assert_claim(conn, job_id, owner_token, claim_fence)
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise ValueError(f"Unknown job id: {job_id}")
            updated = self._update_job_row(
                conn,
                row,
                job_id=job_id,
                state=state,
                staged_path=staged_path,
                status_message=status_message,
            )
        return self._row_to_job(updated)

    @staticmethod
    def _update_job_row(
        conn: sqlite3.Connection,
        current: sqlite3.Row,
        *,
        job_id: int,
        state: JobState | None,
        staged_path: Path | None,
        status_message: str | None,
    ) -> sqlite3.Row:
        next_state = state or JobState(current["state"])
        next_staged_path = (
            staged_path
            if staged_path is not None
            else (
                None if current["staged_path"] is None else Path(current["staged_path"])
            )
        )
        next_message = (
            status_message
            if status_message is not None
            else current["status_message"]
        )
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
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    def replace_job_files(
        self,
        job_id: int,
        files: list[tuple[Path, Path, JobFileState, str | None]],
        *,
        claim: JobClaim | None = None,
    ) -> list[JobFile]:
        """Reconcile a scan without erasing processing or move history."""
        if self.get_job(job_id) is None:
            raise ValueError(f"Unknown job id: {job_id}")

        preserved_states = {
            JobFileState.MOVING,
            JobFileState.STAGED,
            JobFileState.NEEDS_ATTENTION,
        }
        with self._connect() as conn:
            if claim is not None:
                self._assert_claim(conn, job_id, claim.owner_token, claim.fence)
            existing_rows = conn.execute(
                "SELECT * FROM job_files WHERE job_id = ?",
                (job_id,),
            ).fetchall()
            journal_file_ids = {
                row["job_file_id"]
                for row in conn.execute(
                    """
                    SELECT DISTINCT job_file_id
                      FROM move_operations
                     WHERE job_id = ? AND job_file_id IS NOT NULL
                    """,
                    (job_id,),
                ).fetchall()
            }
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
                if current_state in preserved_states or current["id"] in journal_file_ids:
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
                if (
                    JobFileState(row["state"]) in preserved_states
                    or row["id"] in journal_file_ids
                ):
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
            row = conn.execute(
                "SELECT * FROM job_files WHERE id = ?", (file_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"Unknown job file id: {file_id}")
            updated = self._update_job_file_row(
                conn,
                row,
                file_id=file_id,
                state=state,
                staged_path=staged_path,
                status_message=status_message,
            )
        return self._row_to_job_file(updated)

    def update_job_file_claimed(
        self,
        file_id: int,
        *,
        owner_token: str,
        claim_fence: int,
        state: JobFileState | None = None,
        staged_path: Path | None = None,
        status_message: str | None = None,
    ) -> JobFile:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM job_files WHERE id = ?", (file_id,)
            ).fetchone()
            if row is None:
                raise ValueError(f"Unknown job file id: {file_id}")
            self._assert_claim(conn, row["job_id"], owner_token, claim_fence)
            updated = self._update_job_file_row(
                conn,
                row,
                file_id=file_id,
                state=state,
                staged_path=staged_path,
                status_message=status_message,
            )
        return self._row_to_job_file(updated)

    @staticmethod
    def _update_job_file_row(
        conn: sqlite3.Connection,
        current: sqlite3.Row,
        *,
        file_id: int,
        state: JobFileState | None,
        staged_path: Path | None,
        status_message: str | None,
    ) -> sqlite3.Row:
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
        return conn.execute("SELECT * FROM job_files WHERE id = ?", (file_id,)).fetchone()

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

    def claim_job(
        self,
        job_id: int,
        owner_token: str,
        *,
        lease_seconds: float = 300.0,
        now: float | None = None,
    ) -> JobClaim | None:
        current_time = time.time() if now is None else now
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = conn.execute("SELECT id FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if job is None:
                raise ValueError(f"Unknown job id: {job_id}")
            current = conn.execute(
                "SELECT * FROM job_claims WHERE job_id = ?", (job_id,)
            ).fetchone()
            if current is not None and float(current["lease_until"]) > current_time:
                conn.rollback()
                return None

            fence = 1 if current is None else int(current["fence"]) + 1
            lease_until = current_time + lease_seconds
            if current is None:
                conn.execute(
                    """
                    INSERT INTO job_claims
                        (job_id, owner_token, fence, claimed_at, lease_until)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (job_id, owner_token, fence, current_time, lease_until),
                )
            else:
                conn.execute(
                    """
                    UPDATE job_claims
                       SET owner_token = ?, fence = ?, claimed_at = ?, lease_until = ?
                     WHERE job_id = ?
                    """,
                    (owner_token, fence, current_time, lease_until, job_id),
                )
            return JobClaim(job_id, owner_token, fence, lease_until)

    def renew_job_claim(
        self,
        claim: JobClaim,
        *,
        lease_seconds: float = 300.0,
        now: float | None = None,
    ) -> JobClaim:
        current_time = time.time() if now is None else now
        lease_until = current_time + lease_seconds
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE job_claims
                   SET claimed_at = ?, lease_until = ?
                 WHERE job_id = ? AND owner_token = ? AND fence = ?
                   AND lease_until > ?
                """,
                (
                    current_time,
                    lease_until,
                    claim.job_id,
                    claim.owner_token,
                    claim.fence,
                    current_time,
                ),
            )
            if cursor.rowcount != 1:
                raise JobClaimError(f"Job claim expired for job {claim.job_id}")
        return JobClaim(claim.job_id, claim.owner_token, claim.fence, lease_until)

    def release_job_claim(self, claim: JobClaim) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM job_claims
                 WHERE job_id = ? AND owner_token = ? AND fence = ?
                """,
                (claim.job_id, claim.owner_token, claim.fence),
            )

    def begin_move_operation(
        self,
        *,
        job_id: int,
        job_file_id: int | None,
        operation_type: MoveOperationType,
        source_path: Path,
        destination_path: Path,
        source_size: int,
        source_sha256: str,
        prior_file_state: JobFileState | None,
        claim: JobClaim,
    ) -> MoveOperation:
        with self._connect() as conn:
            self._assert_claim(conn, job_id, claim.owner_token, claim.fence)
            if job_file_id is not None:
                current = conn.execute(
                    "SELECT state FROM job_files WHERE id = ? AND job_id = ?",
                    (job_file_id, job_id),
                ).fetchone()
                if current is None:
                    raise ValueError(f"Unknown job file id: {job_file_id}")
                if JobFileState(current["state"]) not in {
                    JobFileState.DISCOVERED,
                    JobFileState.NEEDS_ATTENTION,
                }:
                    raise ValueError(
                        f"Job file {job_file_id} is not eligible for a move"
                    )
                cursor = conn.execute(
                    """
                    UPDATE job_files
                       SET state = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?
                       AND state IN (?, ?)
                    """,
                    (
                        JobFileState.MOVING.value,
                        "Move intent persisted; awaiting filesystem move",
                        job_file_id,
                        JobFileState.DISCOVERED.value,
                        JobFileState.NEEDS_ATTENTION.value,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ValueError(f"Job file {job_file_id} changed state")

            cursor = conn.execute(
                """
                INSERT INTO move_operations
                    (job_id, job_file_id, operation_type, phase,
                     source_path, destination_path, source_size, source_sha256,
                     prior_file_state, owner_token, claim_fence, status_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    job_file_id,
                    operation_type.value,
                    MoveOperationPhase.PREPARED.value,
                    str(source_path),
                    str(destination_path),
                    source_size,
                    source_sha256,
                    None if prior_file_state is None else prior_file_state.value,
                    claim.owner_token,
                    claim.fence,
                    "Move intent persisted; awaiting filesystem move",
                ),
            )
            row = conn.execute(
                "SELECT * FROM move_operations WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()
        return self._row_to_move_operation(row)

    def mark_move_renamed(self, operation_id: int, claim: JobClaim) -> MoveOperation:
        with self._connect() as conn:
            operation = self._get_operation_row(conn, operation_id)
            self._assert_claim(conn, operation["job_id"], claim.owner_token, claim.fence)
            if MoveOperationPhase(operation["phase"]) is MoveOperationPhase.PREPARED:
                conn.execute(
                    """
                    UPDATE move_operations
                       SET phase = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                     WHERE id = ? AND phase = ?
                    """,
                    (
                        MoveOperationPhase.RENAMED.value,
                        "Filesystem move completed; awaiting database finalization",
                        operation_id,
                        MoveOperationPhase.PREPARED.value,
                    ),
                )
            row = self._get_operation_row(conn, operation_id)
        return self._row_to_move_operation(row)

    def complete_move_operation(
        self,
        operation_id: int,
        *,
        staged_path: Path,
        status_message: str,
        claim: JobClaim,
    ) -> MoveOperation:
        with self._connect() as conn:
            operation = self._get_operation_row(conn, operation_id)
            self._assert_claim(conn, operation["job_id"], claim.owner_token, claim.fence)
            phase = MoveOperationPhase(operation["phase"])
            if phase is MoveOperationPhase.COMPLETED:
                return self._row_to_move_operation(operation)
            if phase not in {
                MoveOperationPhase.PREPARED,
                MoveOperationPhase.RENAMED,
            }:
                raise ValueError(f"Move operation {operation_id} is not pending")

            if operation["job_file_id"] is None:
                conn.execute(
                    """
                    UPDATE jobs
                       SET state = ?, staged_path = ?, status_message = ?,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?
                    """,
                    (
                        JobState.STAGED.value,
                        str(staged_path),
                        status_message,
                        operation["job_id"],
                    ),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE job_files
                       SET state = ?, staged_path = ?, status_message = ?,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = ? AND state = ?
                    """,
                    (
                        JobFileState.STAGED.value,
                        str(staged_path),
                        status_message,
                        operation["job_file_id"],
                        JobFileState.MOVING.value,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ValueError(
                        f"Job file {operation['job_file_id']} is not awaiting finalization"
                    )

            conn.execute(
                """
                UPDATE move_operations
                   SET phase = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (
                    MoveOperationPhase.COMPLETED.value,
                    status_message,
                    operation_id,
                ),
            )
            row = self._get_operation_row(conn, operation_id)
        return self._row_to_move_operation(row)

    def mark_move_needs_attention(
        self,
        operation_id: int,
        *,
        status_message: str,
        claim: JobClaim,
        update_parent: bool,
    ) -> MoveOperation:
        with self._connect() as conn:
            operation = self._get_operation_row(conn, operation_id)
            self._assert_claim(conn, operation["job_id"], claim.owner_token, claim.fence)
            if operation["job_file_id"] is not None:
                conn.execute(
                    """
                    UPDATE job_files
                       SET state = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                     WHERE id = ? AND state = ?
                    """,
                    (
                        JobFileState.NEEDS_ATTENTION.value,
                        status_message,
                        operation["job_file_id"],
                        JobFileState.MOVING.value,
                    ),
                )
            if update_parent:
                conn.execute(
                    """
                    UPDATE jobs
                       SET state = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?
                    """,
                    (
                        JobState.NEEDS_ATTENTION.value,
                        status_message,
                        operation["job_id"],
                    ),
                )
            conn.execute(
                """
                UPDATE move_operations
                   SET phase = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND phase IN (?, ?)
                """,
                (
                    MoveOperationPhase.NEEDS_ATTENTION.value,
                    status_message,
                    operation_id,
                    MoveOperationPhase.PREPARED.value,
                    MoveOperationPhase.RENAMED.value,
                ),
            )
            row = self._get_operation_row(conn, operation_id)
        return self._row_to_move_operation(row)

    def recover_move_not_moved(
        self,
        operation_id: int,
        *,
        status_message: str,
        claim: JobClaim,
    ) -> MoveOperation:
        with self._connect() as conn:
            operation = self._get_operation_row(conn, operation_id)
            self._assert_claim(conn, operation["job_id"], claim.owner_token, claim.fence)
            if operation["job_file_id"] is not None:
                prior_state = operation["prior_file_state"] or JobFileState.DISCOVERED.value
                conn.execute(
                    """
                    UPDATE job_files
                       SET state = ?, staged_path = NULL, status_message = ?,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = ? AND state = ?
                    """,
                    (
                        prior_state,
                        status_message,
                        operation["job_file_id"],
                        JobFileState.MOVING.value,
                    ),
                )
            conn.execute(
                """
                UPDATE move_operations
                   SET phase = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND phase IN (?, ?)
                """,
                (
                    MoveOperationPhase.NOT_MOVED.value,
                    status_message,
                    operation_id,
                    MoveOperationPhase.PREPARED.value,
                    MoveOperationPhase.RENAMED.value,
                ),
            )
            row = self._get_operation_row(conn, operation_id)
        return self._row_to_move_operation(row)

    def recover_move_as_staged(
        self,
        operation_id: int,
        *,
        staged_path: Path,
        status_message: str,
        claim: JobClaim,
    ) -> MoveOperation:
        with self._connect() as conn:
            operation = self._get_operation_row(conn, operation_id)
            self._assert_claim(conn, operation["job_id"], claim.owner_token, claim.fence)
            if operation["job_file_id"] is None:
                conn.execute(
                    """
                    UPDATE jobs
                       SET state = ?, staged_path = ?, status_message = ?,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?
                    """,
                    (
                        JobState.STAGED.value,
                        str(staged_path),
                        status_message,
                        operation["job_id"],
                    ),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE job_files
                       SET state = ?, staged_path = ?, status_message = ?,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = ? AND state = ?
                    """,
                    (
                        JobFileState.STAGED.value,
                        str(staged_path),
                        status_message,
                        operation["job_file_id"],
                        JobFileState.MOVING.value,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ValueError(
                        f"Job file {operation['job_file_id']} is not awaiting recovery"
                    )
            conn.execute(
                """
                UPDATE move_operations
                   SET phase = ?, status_message = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND phase IN (?, ?)
                """,
                (
                    MoveOperationPhase.COMPLETED.value,
                    status_message,
                    operation_id,
                    MoveOperationPhase.PREPARED.value,
                    MoveOperationPhase.RENAMED.value,
                ),
            )
            row = self._get_operation_row(conn, operation_id)
        return self._row_to_move_operation(row)

    def list_pending_move_operations(self, job_id: int) -> list[MoveOperation]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM move_operations
                 WHERE job_id = ? AND phase IN (?, ?)
                 ORDER BY id
                """,
                (
                    job_id,
                    MoveOperationPhase.PREPARED.value,
                    MoveOperationPhase.RENAMED.value,
                ),
            ).fetchall()
        return [self._row_to_move_operation(row) for row in rows]

    def list_move_operations(self, job_id: int) -> list[MoveOperation]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM move_operations WHERE job_id = ? ORDER BY id",
                (job_id,),
            ).fetchall()
        return [self._row_to_move_operation(row) for row in rows]

    def get_move_operation(self, operation_id: int) -> MoveOperation | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM move_operations WHERE id = ?", (operation_id,)
            ).fetchone()
        return None if row is None else self._row_to_move_operation(row)

    def _assert_claim(
        self,
        conn: sqlite3.Connection,
        job_id: int,
        owner_token: str,
        claim_fence: int,
    ) -> None:
        row = conn.execute(
            "SELECT * FROM job_claims WHERE job_id = ?", (job_id,)
        ).fetchone()
        if (
            row is None
            or row["owner_token"] != owner_token
            or int(row["fence"]) != claim_fence
            or float(row["lease_until"]) <= time.time()
        ):
            raise JobClaimError(f"Job claim is not active for job {job_id}")

    @staticmethod
    def _get_operation_row(conn: sqlite3.Connection, operation_id: int) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM move_operations WHERE id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown move operation id: {operation_id}")
        return row

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

    @staticmethod
    def _row_to_move_operation(row: sqlite3.Row) -> MoveOperation:
        prior_state = row["prior_file_state"]
        return MoveOperation(
            id=row["id"],
            job_id=row["job_id"],
            job_file_id=row["job_file_id"],
            operation_type=MoveOperationType(row["operation_type"]),
            phase=MoveOperationPhase(row["phase"]),
            source_path=Path(row["source_path"]),
            destination_path=Path(row["destination_path"]),
            source_size=row["source_size"],
            source_sha256=row["source_sha256"],
            prior_file_state=None if prior_state is None else JobFileState(prior_state),
            status_message=row["status_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
