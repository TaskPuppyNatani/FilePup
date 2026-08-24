import errno
import os
from dataclasses import dataclass
from pathlib import Path

from .config import FilePupConfig
from .database import JobStore
from .inventory import InventoryEngine
from .jobs import Job, JobFileState, JobState


@dataclass(frozen=True)
class IntakeResult:
    job: Job
    moved: bool
    message: str


class IntakeEngine:
    """Move completed torrent content into Staging without overwriting anything.

    Single-file intake uses an atomic rename. Directory intake first inventories
    the torrent, preflights the complete supported-media batch, then moves one
    child at a time while recording child state after each verified move.

    Cross-filesystem copy-and-delete remains intentionally disabled.
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

        # Never create a media mount path automatically. A missing Staging path
        # could mean the expected disk/share is not mounted.
        if not staging_root.is_dir():
            return self._attention(
                job,
                f"Staging directory is unavailable: {staging_root}",
            )

        if source.is_dir():
            return self._stage_directory(job, source, staging_root)

        return self._stage_single(job, source, staging_root)

    def _stage_single(self, job: Job, source: Path, staging_root: Path) -> IntakeResult:
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

    def _stage_directory(
        self,
        job: Job,
        source_root: Path,
        staging_root: Path,
    ) -> IntakeResult:
        inventory = InventoryEngine(self.store).inventory(job.id)
        supported = [
            file for file in inventory.files if file.state is JobFileState.DISCOVERED
        ]

        if not supported:
            return self._attention(
                job,
                "Directory contains no supported media files; source preserved",
            )

        planned: list[tuple[object, Path, Path]] = []
        for file in supported:
            file_source = file.source_path.expanduser().resolve()
            if not self._is_within(file_source, source_root):
                return self._attention(
                    job,
                    f"Inventoried file escapes source directory; refusing batch: {file_source}",
                )
            if not file_source.is_file() or file_source.is_symlink():
                return self._attention(
                    job,
                    f"Inventoried source is unavailable or unsafe: {file_source}",
                )

            destination = (staging_root / file.relative_path).resolve()
            if not self._is_within(destination, staging_root):
                return self._attention(
                    job,
                    f"Planned destination escapes Staging; refusing batch: {destination}",
                )
            if destination.exists():
                return self._attention(
                    job,
                    f"Destination already exists; refusing entire batch: {destination}",
                )
            if file_source.stat().st_dev != staging_root.stat().st_dev:
                return self._attention(
                    job,
                    "Completed Torrents and Staging are on different filesystems; "
                    "cross-filesystem copy-and-delete is intentionally disabled",
                )
            planned.append((file, file_source, destination))

        # Creating empty destination directories is harmless and ensures a
        # mkdir failure happens before any source media is moved.
        try:
            for _, _, destination in planned:
                destination.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return self._attention(job, f"Could not prepare Staging directories: {exc}")

        moved_count = 0
        for file, file_source, destination in planned:
            try:
                os.rename(file_source, destination)
            except OSError as exc:
                message = (
                    f"Batch stopped after {moved_count} verified move(s); "
                    f"failed moving {file.relative_path}: {exc}"
                )
                self.store.update_job_file(
                    file.id,
                    state=JobFileState.NEEDS_ATTENTION,
                    status_message=message,
                )
                return self._attention(job, message)

            if not destination.exists() or file_source.exists():
                message = (
                    f"Batch stopped after {moved_count} verified move(s); "
                    f"post-move verification failed for {file.relative_path}"
                )
                self.store.update_job_file(
                    file.id,
                    state=JobFileState.NEEDS_ATTENTION,
                    status_message=message,
                )
                return self._attention(job, message)

            self.store.update_job_file(
                file.id,
                state=JobFileState.STAGED,
                staged_path=destination,
                status_message="Moved to Staging and verified",
            )
            moved_count += 1

        updated = self.store.update_job(
            job.id,
            state=JobState.STAGED,
            staged_path=staging_root,
            status_message=(
                f"Staged {moved_count} supported media file(s); "
                f"preserved {inventory.ignored_count} ignored file(s) in source"
            ),
        )
        return IntakeResult(
            updated,
            True,
            f"Staged {moved_count} supported media file(s)",
        )

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
