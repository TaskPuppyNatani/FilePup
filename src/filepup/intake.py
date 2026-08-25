import ctypes
import errno
import os
from dataclasses import dataclass
from pathlib import Path

from .config import FilePupConfig
from .database import JobStore
from .inventory import InventoryEngine, SUPPORTED_MEDIA_EXTENSIONS
from .jobs import Job, JobFileState, JobState
from .safety import SafetyController


@dataclass(frozen=True)
class IntakeResult:
    job: Job
    moved: bool
    message: str


class IntakeEngine:
    """Move completed torrent content into Staging without unsafe cleanup.

    Single-file intake uses an atomic rename. Directory intake inventories the
    torrent and then processes supported child files independently, preserving
    relative paths and recording the outcome of every child.

    Destination conflicts never overwrite existing files. Verified duplicates
    are recognized, but source deletion remains controlled exclusively by
    SafetyController and is currently hard-disabled. Cross-filesystem
    copy-and-delete is also intentionally disabled.
    """

    def __init__(self, config: FilePupConfig, store: JobStore):
        self.config = config
        self.store = store
        self.safety = SafetyController()

    def stage(self, job_id: int) -> IntakeResult:
        """Stage a newly discovered job.

        Normal staging is deliberately limited to DISCOVERED jobs.  A job
        that needs attention must go through the explicit retry transition so
        callers cannot accidentally re-run a terminal or already successful
        job.
        """
        return self._process(job_id, expected_state=JobState.DISCOVERED, retry=False)

    def retry(self, job_id: int) -> IntakeResult:
        """Retry only the unresolved work for a job needing attention."""
        return self._process(
            job_id,
            expected_state=JobState.NEEDS_ATTENTION,
            retry=True,
        )

    def _process(
        self,
        job_id: int,
        *,
        expected_state: JobState,
        retry: bool,
    ) -> IntakeResult:
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError(f"Unknown job id: {job_id}")

        source_candidate = job.source_path.expanduser()
        if source_candidate.is_symlink():
            return self._attention(
                job,
                f"Source path is a symlink and will not be staged: {source_candidate}",
            )

        source = source_candidate.resolve()
        completed_root = self.config.completed_torrents.expanduser().resolve()
        staging_root = self.config.staging.expanduser().resolve()

        if job.state is not expected_state:
            if retry:
                message = (
                    "Job is not awaiting retry; "
                    f"current state is {job.state.value}"
                )
            else:
                message = f"Job is already {job.state.value}"
            return IntakeResult(job, False, message)

        if not self._is_within(source, completed_root):
            return self._attention(
                job,
                f"Source is outside Completed Torrents: {source}",
            )

        if not source.exists():
            return self._attention(job, f"Source does not exist: {source}")

        # Never create a media mount path automatically. A missing Staging path
        # could mean the expected disk/share is not mounted.
        if not staging_root.is_dir():
            return self._attention(
                job,
                f"Staging directory is unavailable: {staging_root}",
            )

        if source.is_dir():
            return self._stage_directory(job, source, staging_root, retry=retry)

        return self._stage_single(job, source, staging_root)

    def _stage_single(self, job: Job, source: Path, staging_root: Path) -> IntakeResult:
        if source.suffix.lower() not in SUPPORTED_MEDIA_EXTENSIONS:
            updated = self.store.update_job(
                job.id,
                state=JobState.COMPLETE,
                status_message="Unsupported extension; source preserved",
            )
            return IntakeResult(updated, False, "Unsupported extension; source preserved")

        planned_destination = staging_root / source.name
        destination = planned_destination.resolve()
        if not self._is_within(destination, staging_root):
            return self._attention(
                job,
                f"Planned destination escapes Staging: {destination}",
            )

        if os.path.lexists(planned_destination):
            if planned_destination.is_symlink():
                return self._attention(
                    job,
                    "Destination is an existing symlink; source preserved: "
                    f"{planned_destination}",
                )
            if self._files_identical(source, destination):
                decision = self.safety.may_delete_source(source)
                return self._attention(
                    job,
                    "Duplicate destination verified but source deletion was refused: "
                    f"{decision.reason}",
                )
            return self._attention(
                job,
                f"Destination already exists but is not identical; source preserved: {destination}",
            )

        try:
            self._move_without_replacing(source, destination)
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                return self._attention(
                    job,
                    "Completed Torrents and Staging are on different filesystems; "
                    "cross-filesystem copy-and-delete is intentionally disabled",
                )
            if exc.errno == errno.EEXIST:
                return self._attention(
                    job,
                    "Destination appeared during the staging move; source preserved: "
                    f"{destination}",
                )
            return self._attention(job, f"Staging move failed: {exc}")

        if not destination.exists() or source.exists():
            return self._attention(
                job,
                "Move returned but post-move verification failed; manual inspection required",
            )

        updated = self.store.update_job(
            job.id,
            state=JobState.STAGED,
            staged_path=destination,
            status_message="Moved to Staging and verified",
        )
        return IntakeResult(updated, True, f"Staged at {destination}")

    def _stage_directory(
        self,
        job: Job,
        source_root: Path,
        staging_root: Path,
        *,
        retry: bool,
    ) -> IntakeResult:
        inventory = InventoryEngine(self.store).inventory(job.id)
        candidate_states = {JobFileState.DISCOVERED}
        if retry:
            candidate_states.add(JobFileState.NEEDS_ATTENTION)
        supported = [
            file for file in inventory.files if file.state in candidate_states
        ]

        previously_staged_count = sum(
            file.state is JobFileState.STAGED for file in inventory.files
        )

        moved_count = 0
        duplicate_count = 0

        for file in supported:
            source_candidate = file.source_path.expanduser()
            if source_candidate.is_symlink():
                self._file_attention(
                    file.id,
                    "Inventoried source is a symlink and will not be staged: "
                    f"{source_candidate}",
                )
                continue

            file_source = source_candidate.resolve()
            if not self._is_within(file_source, source_root):
                self._file_attention(
                    file.id,
                    f"Inventoried file escapes source directory: {file_source}",
                )
                continue

            if not file_source.is_file():
                self._file_attention(
                    file.id,
                    f"Inventoried source is unavailable or unsafe: {file_source}",
                )
                continue

            planned_destination = staging_root / file.relative_path
            destination = planned_destination.resolve()
            if not self._is_within(destination, staging_root):
                self._file_attention(
                    file.id,
                    f"Planned destination escapes Staging: {destination}",
                )
                continue

            if os.path.lexists(planned_destination):
                if planned_destination.is_symlink():
                    self._file_attention(
                        file.id,
                        "Destination is an existing symlink; source preserved: "
                        f"{planned_destination}",
                    )
                    continue
                if self._files_identical(file_source, destination):
                    decision = self.safety.may_delete_source(file_source)
                    self._file_attention(
                        file.id,
                        "Duplicate destination verified but source deletion was refused: "
                        f"{decision.reason}",
                    )
                    duplicate_count += 1
                else:
                    self._file_attention(
                        file.id,
                        "Destination already exists but is not identical; source preserved: "
                        f"{destination}",
                    )
                continue

            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                self._file_attention(
                    file.id,
                    f"Could not prepare destination directory: {exc}",
                )
                continue

            try:
                self._move_without_replacing(file_source, destination)
            except OSError as exc:
                if exc.errno == errno.EXDEV:
                    message = (
                        "Source and Staging are on different filesystems; "
                        "cross-filesystem copy-and-delete is intentionally disabled"
                    )
                elif exc.errno == errno.EEXIST:
                    message = (
                        "Destination appeared during the staging move; "
                        f"source preserved: {destination}"
                    )
                else:
                    message = f"Staging move failed: {exc}"
                self._file_attention(file.id, message)
                continue

            if not destination.exists() or file_source.exists():
                self._file_attention(
                    file.id,
                    f"Post-move verification failed for {file.relative_path}",
                )
                continue

            self.store.update_job_file(
                file.id,
                state=JobFileState.STAGED,
                staged_path=destination,
                status_message="Moved to Staging and verified",
            )
            moved_count += 1

        final_files = self.store.list_job_files(job.id)
        ignored_count = sum(
            file.state is JobFileState.IGNORED for file in final_files
        )
        supported_files = [
            file for file in final_files if file.state is not JobFileState.IGNORED
        ]
        unresolved_files = [
            file for file in supported_files if file.state is not JobFileState.STAGED
        ]

        if not supported_files:
            message = "Directory contains no supported media files; source preserved"
            updated = self.store.update_job(
                job.id,
                state=JobState.COMPLETE,
                status_message=message,
            )
            return IntakeResult(updated, False, message)

        if unresolved_files:
            message = (
                f"Staged {moved_count} supported media file(s); "
                f"{len(unresolved_files)} file(s) need attention; "
                f"preserved {ignored_count} ignored file(s)"
            )
            if previously_staged_count:
                message += f"; {previously_staged_count} already staged"
            if duplicate_count:
                message += (
                    f"; {duplicate_count} verified duplicate(s) preserved; "
                    "source deletion is hard-disabled"
                )
            updated = self.store.update_job(
                job.id,
                state=JobState.NEEDS_ATTENTION,
                staged_path=staging_root if moved_count else None,
                status_message=message,
            )
            return IntakeResult(updated, moved_count > 0, message)

        message = (
            f"Staged {moved_count} supported media file(s); "
            f"preserved {ignored_count} ignored file(s) in source"
        )
        if previously_staged_count:
            message += f"; {previously_staged_count} already staged"
        updated = self.store.update_job(
            job.id,
            state=JobState.STAGED,
            staged_path=staging_root,
            status_message=message,
        )
        return IntakeResult(
            updated,
            moved_count > 0,
            message,
        )

    def _file_attention(self, file_id: int, message: str) -> None:
        self.store.update_job_file(
            file_id,
            state=JobFileState.NEEDS_ATTENTION,
            status_message=message,
        )

    def _attention(self, job: Job, message: str) -> IntakeResult:
        updated = self.store.update_job(
            job.id,
            state=JobState.NEEDS_ATTENTION,
            status_message=message,
        )
        return IntakeResult(updated, False, message)

    @staticmethod
    def _files_identical(source: Path, destination: Path) -> bool:
        if not source.is_file() or not destination.is_file():
            return False
        if source.stat().st_size != destination.stat().st_size:
            return False

        chunk_size = 1024 * 1024
        with source.open("rb") as src, destination.open("rb") as dst:
            while True:
                src_chunk = src.read(chunk_size)
                dst_chunk = dst.read(chunk_size)
                if src_chunk != dst_chunk:
                    return False
                if not src_chunk:
                    return True

    @staticmethod
    def _move_without_replacing(source: Path, destination: Path) -> None:
        """Atomically rename a source without replacing an existing path.

        FilePup only supports Linux media mounts.  ``renameat2`` with
        ``RENAME_NOREPLACE`` keeps the source-removing operation a rename while
        closing the check-then-rename overwrite race.  If the primitive is not
        available, fail safely instead of falling back to clobbering rename.
        """
        libc = ctypes.CDLL(None, use_errno=True)
        try:
            renameat2 = libc.renameat2
        except AttributeError as exc:
            raise OSError(
                errno.ENOTSUP,
                "Atomic no-replace rename is unavailable",
                str(destination),
            ) from exc

        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(
            -100,  # AT_FDCWD
            os.fsencode(source),
            -100,  # AT_FDCWD
            os.fsencode(destination),
            1,  # RENAME_NOREPLACE
        )
        if result != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), str(destination))

    @staticmethod
    def _is_within(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
        except ValueError:
            return False
        return True
