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
        if source.suffix.lower() not in SUPPORTED_MEDIA_EXTENSIONS:
            updated = self.store.update_job(
                job.id,
                state=JobState.COMPLETE,
                status_message="Unsupported extension; source preserved",
            )
            return IntakeResult(updated, False, "Unsupported extension; source preserved")

        destination = staging_root / source.name
        if destination.exists():
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
            updated = self.store.update_job(
                job.id,
                state=JobState.COMPLETE,
                status_message="Directory contains no supported media files; source preserved",
            )
            return IntakeResult(
                updated,
                False,
                "Directory contains no supported media files; source preserved",
            )

        moved_count = 0
        failed_count = 0
        duplicate_count = 0

        for file in supported:
            file_source = file.source_path.expanduser().resolve()
            if not self._is_within(file_source, source_root):
                self._file_attention(
                    file.id,
                    f"Inventoried file escapes source directory: {file_source}",
                )
                failed_count += 1
                continue

            if not file_source.is_file() or file_source.is_symlink():
                self._file_attention(
                    file.id,
                    f"Inventoried source is unavailable or unsafe: {file_source}",
                )
                failed_count += 1
                continue

            destination = (staging_root / file.relative_path).resolve()
            if not self._is_within(destination, staging_root):
                self._file_attention(
                    file.id,
                    f"Planned destination escapes Staging: {destination}",
                )
                failed_count += 1
                continue

            if destination.exists():
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
                failed_count += 1
                continue

            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                self._file_attention(
                    file.id,
                    f"Could not prepare destination directory: {exc}",
                )
                failed_count += 1
                continue

            try:
                os.rename(file_source, destination)
            except OSError as exc:
                if exc.errno == errno.EXDEV:
                    message = (
                        "Source and Staging are on different filesystems; "
                        "cross-filesystem copy-and-delete is intentionally disabled"
                    )
                else:
                    message = f"Staging move failed: {exc}"
                self._file_attention(file.id, message)
                failed_count += 1
                continue

            if not destination.exists() or file_source.exists():
                self._file_attention(
                    file.id,
                    f"Post-move verification failed for {file.relative_path}",
                )
                failed_count += 1
                continue

            self.store.update_job_file(
                file.id,
                state=JobFileState.STAGED,
                staged_path=destination,
                status_message="Moved to Staging and verified",
            )
            moved_count += 1

        if failed_count:
            message = (
                f"Staged {moved_count} supported media file(s); "
                f"{failed_count} file(s) need attention; "
                f"preserved {inventory.ignored_count} ignored file(s)"
            )
            if duplicate_count:
                message += f"; {duplicate_count} verified duplicate(s) preserved"
            updated = self.store.update_job(
                job.id,
                state=JobState.NEEDS_ATTENTION,
                staged_path=staging_root if moved_count else None,
                status_message=message,
            )
            return IntakeResult(updated, moved_count > 0, message)

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
    def _is_within(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
        except ValueError:
            return False
        return True
