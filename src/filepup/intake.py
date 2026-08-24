import errno
import os
from dataclasses import dataclass
from pathlib import Path

from .config import FilePupConfig
from .database import JobStore
from .jobs import Job, JobState


@dataclass(frozen=True)
class IntakeResult:
    job: Job
    moved: bool
    message: str


class IntakeEngine:
    """Move completed torrent content into Staging without overwriting anything.

    This first implementation deliberately uses an atomic rename only. If
    Completed Torrents and Staging are on different filesystems, FilePup stops
    the job instead of silently falling back to copy-and-delete behavior.

    Multi-file torrent directories are also refused for now. FilePup must gain
    per-file inventory/state tracking before it is allowed to move files out of
    a directory piecemeal.
    """

    def __init__(self, config: FilePupConfig, store: JobStore):
        self.config = config
        self.store = store

    def stage(self, job_id: int) -> IntakeResult:
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError(f"Unknown job id: {job_id}")

        source = job.source_path.expanduser().resolve()
        completed_root = self.config.completed_torrents.expanduser().resolve()
        staging_root = self.config.staging.expanduser().resolve()

        if job.state is not JobState.DISCOVERED:
            return IntakeResult(job, False, f"Job is already {job.state.value}")

        if not self._is_within(source, completed_root):
            return self._attention(
                job,
                f"Source is outside Completed Torrents: {source}",
            )

        if not source.exists():
            return self._attention(job, f"Source does not exist: {source}")

        if source.is_dir():
            return self._attention(
                job,
                "Directory torrent intake is not implemented yet; source preserved",
            )

        # Never create a media mount path automatically. A missing Staging path
        # could mean the expected disk/share is not mounted.
        if not staging_root.is_dir():
            return self._attention(
                job,
                f"Staging directory is unavailable: {staging_root}",
            )

        destination = staging_root / source.name
        if destination.exists():
            return self._attention(
                job,
                f"Destination already exists; refusing overwrite: {destination}",
            )

        try:
            os.rename(source, destination)
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                return self._attention(
                    job,
                    "Completed Torrents and Staging are on different filesystems; "
                    "cross-filesystem copy-and-delete is intentionally disabled",
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

    def _attention(self, job: Job, message: str) -> IntakeResult:
        updated = self.store.update_job(
            job.id,
            state=JobState.NEEDS_ATTENTION,
            status_message=message,
        )
        return IntakeResult(updated, False, message)

    @staticmethod
    def _is_within(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
        except ValueError:
            return False
        return True
