import ctypes
import errno
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from .config import FilePupConfig
from .database import JobBusyError, JobClaimError, JobStore
from .inventory import InventoryEngine, SUPPORTED_MEDIA_EXTENSIONS
from .jobs import (
    Job,
    JobClaim,
    JobFileState,
    JobState,
    MoveOperation,
    MoveOperationPhase,
    MoveOperationType,
)
from .safety import SafetyController


@dataclass(frozen=True)
class IntakeResult:
    job: Job
    moved: bool
    message: str


class IntakeEngine:
    """Move completed torrent content into Staging without unsafe cleanup.

    Every source-removing move has a durable ``PREPARED`` journal row before
    the filesystem rename. Recovery uses the recorded source fingerprint and
    the two paths to decide whether the move did not happen, did happen, or is
    ambiguous. Recovery never deletes or overwrites either path.
    """

    def __init__(self, config: FilePupConfig, store: JobStore):
        self.config = config
        self.store = store
        self.safety = SafetyController()

    def stage(self, job_id: int) -> IntakeResult:
        """Stage a newly discovered job."""
        return self._process(job_id, expected_state=JobState.DISCOVERED, retry=False)

    def retry(self, job_id: int) -> IntakeResult:
        """Retry unresolved work after reconciling interrupted moves first."""
        return self._process(
            job_id,
            expected_state=JobState.NEEDS_ATTENTION,
            retry=True,
        )

    def recover(self, job_id: int) -> IntakeResult:
        """Reconcile only journaled interrupted moves for a job."""
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError(f"Unknown job id: {job_id}")

        try:
            with self.store.job_lock(job_id):
                claim = self.store.claim_job(job_id, uuid4().hex)
                if claim is None:
                    return IntakeResult(
                        self.store.get_job(job_id) or job,
                        False,
                        "Job is already being processed; no filesystem changes made",
                    )
                try:
                    return self._recover_claimed(job_id, claim)
                finally:
                    self.store.release_job_claim(claim)
        except JobBusyError:
            return IntakeResult(
                self.store.get_job(job_id) or job,
                False,
                "Job is already being processed; no filesystem changes made",
            )
        except JobClaimError as exc:
            return IntakeResult(
                self.store.get_job(job_id) or job,
                False,
                f"Job processing claim was lost; recover again: {exc}",
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

        try:
            with self.store.job_lock(job_id):
                claim = self.store.claim_job(job_id, uuid4().hex)
                if claim is None:
                    return IntakeResult(
                        self.store.get_job(job_id) or job,
                        False,
                        "Job is already being processed; no filesystem changes made",
                    )
                try:
                    return self._process_claimed(
                        job_id,
                        expected_state=expected_state,
                        retry=retry,
                        claim=claim,
                    )
                finally:
                    self.store.release_job_claim(claim)
        except JobBusyError:
            return IntakeResult(
                self.store.get_job(job_id) or job,
                False,
                "Job is already being processed; no filesystem changes made",
            )
        except JobClaimError as exc:
            return IntakeResult(
                self.store.get_job(job_id) or job,
                False,
                f"Job processing claim was lost; recover again: {exc}",
            )

    def _process_claimed(
        self,
        job_id: int,
        *,
        expected_state: JobState,
        retry: bool,
        claim: JobClaim,
    ) -> IntakeResult:
        job = self._required_job(job_id)
        staging_root = self.config.staging.expanduser().resolve()

        # Recovery is deliberately before the state gate. A crash can leave a
        # DISCOVERED parent with a destination that already contains the file.
        claim = self._renew(claim)
        self._reconcile_pending_moves(job_id, staging_root, claim)
        claim = self._settle_directory_parent(
            job_id,
            staging_root,
            claim,
            allow_staged=True,
        )

        job = self._required_job(job_id)
        if job.state is not expected_state:
            if retry:
                message = (
                    "Job is not awaiting retry; "
                    f"current state is {job.state.value}"
                )
            else:
                message = f"Job is already {job.state.value}"
            return IntakeResult(job, False, message)

        source_candidate = job.source_path.expanduser()
        if source_candidate.is_symlink():
            return self._attention(
                job,
                f"Source path is a symlink and will not be staged: {source_candidate}",
                claim,
            )

        source = source_candidate.resolve()
        completed_root = self.config.completed_torrents.expanduser().resolve()

        if not self._is_within(source, completed_root):
            return self._attention(
                job,
                f"Source is outside Completed Torrents: {source}",
                claim,
            )

        if not source.exists():
            return self._attention(job, f"Source does not exist: {source}", claim)

        # Never create a media mount path automatically. A missing Staging path
        # could mean the expected disk/share is not mounted.
        if not staging_root.is_dir():
            return self._attention(
                job,
                f"Staging directory is unavailable: {staging_root}",
                claim,
            )

        claim = self._renew(claim)
        if source.is_dir():
            return self._stage_directory(
                job,
                source,
                staging_root,
                retry=retry,
                claim=claim,
            )

        return self._stage_single(job, source, staging_root, claim)

    def _recover_claimed(self, job_id: int, claim: JobClaim) -> IntakeResult:
        staging_root = self.config.staging.expanduser().resolve()
        claim = self._renew(claim)
        pending_count, recovered_count, not_moved_count, attention_count = (
            self._reconcile_pending_moves(job_id, staging_root, claim)
        )
        claim = self._settle_directory_parent(
            job_id,
            staging_root,
            claim,
            allow_staged=True,
        )

        job = self._required_job(job_id)
        pieces: list[str] = []
        if not_moved_count:
            pieces.append(f"confirmed {not_moved_count} move(s) did not happen")
        if pending_count:
            pieces.append(f"reconciled {pending_count} interrupted move(s)")
        if attention_count:
            pieces.append(f"{attention_count} move(s) need attention")
            attention_messages = [
                operation.status_message
                for operation in self.store.list_move_operations(job_id)
                if operation.phase is MoveOperationPhase.NEEDS_ATTENTION
                and operation.status_message
            ]
            pieces.extend(attention_messages[:attention_count])
        if not pieces:
            pieces.append("no pending move operations found")
        return IntakeResult(
            job,
            recovered_count > 0,
            "; ".join(pieces),
        )

    def _stage_single(
        self,
        job: Job,
        source: Path,
        staging_root: Path,
        claim: JobClaim,
    ) -> IntakeResult:
        if source.suffix.lower() not in SUPPORTED_MEDIA_EXTENSIONS:
            updated = self.store.update_job_claimed(
                job.id,
                owner_token=claim.owner_token,
                claim_fence=claim.fence,
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
                claim,
            )

        if os.path.lexists(planned_destination):
            if planned_destination.is_symlink():
                return self._attention(
                    job,
                    "Destination is an existing symlink; source preserved: "
                    f"{planned_destination}",
                    claim,
                )
            if self._files_identical(source, destination):
                decision = self.safety.may_delete_source(source)
                return self._attention(
                    job,
                    "Duplicate destination verified but source deletion was refused: "
                    f"{decision.reason}",
                    claim,
                )
            return self._attention(
                job,
                "Destination already exists but is not identical; source preserved: "
                f"{destination}",
                claim,
            )

        try:
            source_size, source_sha256 = self._fingerprint(source)
        except OSError as exc:
            return self._attention(job, f"Could not fingerprint source: {exc}", claim)

        claim = self._renew(claim)
        operation = self.store.begin_move_operation(
            job_id=job.id,
            job_file_id=None,
            operation_type=MoveOperationType.MOVE_TO_STAGING,
            source_path=source,
            destination_path=destination,
            source_size=source_size,
            source_sha256=source_sha256,
            prior_file_state=None,
            claim=claim,
        )

        try:
            claim = self._renew(claim)
            self._move_without_replacing(source, destination)
        except OSError as exc:
            message = self._move_error(exc, destination)
            self.store.mark_move_needs_attention(
                operation.id,
                status_message=message,
                claim=claim,
                update_parent=True,
            )
            updated = self._required_job(job.id)
            return IntakeResult(updated, False, message)

        if not self._destination_matches(destination, source_size, source_sha256) or source.exists():
            message = "Move returned but post-move verification failed; manual inspection required"
            self.store.mark_move_needs_attention(
                operation.id,
                status_message=message,
                claim=claim,
                update_parent=True,
            )
            updated = self._required_job(job.id)
            return IntakeResult(updated, False, message)

        claim = self._renew(claim)
        self.store.mark_move_renamed(operation.id, claim)
        claim = self._renew(claim)
        self.store.complete_move_operation(
            operation.id,
            staged_path=destination,
            status_message="Moved to Staging and verified",
            claim=claim,
        )
        updated = self._required_job(job.id)
        return IntakeResult(updated, True, f"Staged at {destination}")

    def _stage_directory(
        self,
        job: Job,
        source_root: Path,
        staging_root: Path,
        *,
        retry: bool,
        claim: JobClaim,
    ) -> IntakeResult:
        claim = self._renew(claim)
        try:
            inventory = InventoryEngine(self.store).inventory(job.id, claim=claim)
        except FileNotFoundError:
            return self._attention(
                job,
                f"Source directory disappeared during inventory: {source_root}",
                claim,
            )

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
            claim = self._renew(claim)
            source_candidate = file.source_path.expanduser()
            if source_candidate.is_symlink():
                self._file_attention(
                    file.id,
                    "Inventoried source is a symlink and will not be staged: "
                    f"{source_candidate}",
                    claim,
                )
                continue

            file_source = source_candidate.resolve()
            if not self._is_within(file_source, source_root):
                self._file_attention(
                    file.id,
                    f"Inventoried file escapes source directory: {file_source}",
                    claim,
                )
                continue

            if not file_source.is_file():
                self._file_attention(
                    file.id,
                    f"Inventoried source is unavailable or unsafe: {file_source}",
                    claim,
                )
                continue

            planned_destination = staging_root / file.relative_path
            destination = planned_destination.resolve()
            if not self._is_within(destination, staging_root):
                self._file_attention(
                    file.id,
                    f"Planned destination escapes Staging: {destination}",
                    claim,
                )
                continue

            if os.path.lexists(planned_destination):
                if planned_destination.is_symlink():
                    self._file_attention(
                        file.id,
                        "Destination is an existing symlink; source preserved: "
                        f"{planned_destination}",
                        claim,
                    )
                    continue
                if self._files_identical(file_source, destination):
                    decision = self.safety.may_delete_source(file_source)
                    self._file_attention(
                        file.id,
                        "Duplicate destination verified but source deletion was refused: "
                        f"{decision.reason}",
                        claim,
                    )
                    duplicate_count += 1
                else:
                    self._file_attention(
                        file.id,
                        "Destination already exists but is not identical; source preserved: "
                        f"{destination}",
                        claim,
                    )
                continue

            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                source_size, source_sha256 = self._fingerprint(file_source)
            except OSError as exc:
                self._file_attention(
                    file.id,
                    f"Could not prepare source or destination: {exc}",
                    claim,
                )
                continue

            claim = self._renew(claim)
            operation = self.store.begin_move_operation(
                job_id=job.id,
                job_file_id=file.id,
                operation_type=MoveOperationType.MOVE_TO_STAGING,
                source_path=file_source,
                destination_path=destination,
                source_size=source_size,
                source_sha256=source_sha256,
                prior_file_state=file.state,
                claim=claim,
            )

            try:
                claim = self._renew(claim)
                self._move_without_replacing(file_source, destination)
            except OSError as exc:
                message = self._move_error(exc, destination)
                self.store.mark_move_needs_attention(
                    operation.id,
                    status_message=message,
                    claim=claim,
                    update_parent=False,
                )
                continue

            if not self._destination_matches(
                destination, source_size, source_sha256
            ) or file_source.exists():
                message = f"Post-move verification failed for {file.relative_path}"
                self.store.mark_move_needs_attention(
                    operation.id,
                    status_message=message,
                    claim=claim,
                    update_parent=False,
                )
                continue

            claim = self._renew(claim)
            self.store.mark_move_renamed(operation.id, claim)
            claim = self._renew(claim)
            self.store.complete_move_operation(
                operation.id,
                staged_path=destination,
                status_message="Moved to Staging and verified",
                claim=claim,
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
            updated = self.store.update_job_claimed(
                job.id,
                owner_token=claim.owner_token,
                claim_fence=claim.fence,
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
            updated = self.store.update_job_claimed(
                job.id,
                owner_token=claim.owner_token,
                claim_fence=claim.fence,
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
        updated = self.store.update_job_claimed(
            job.id,
            owner_token=claim.owner_token,
            claim_fence=claim.fence,
            state=JobState.STAGED,
            staged_path=staging_root,
            status_message=message,
        )
        return IntakeResult(updated, moved_count > 0, message)

    def _reconcile_pending_moves(
        self,
        job_id: int,
        staging_root: Path,
        claim: JobClaim,
    ) -> tuple[int, int, int, int]:
        operations = self.store.list_pending_move_operations(job_id)
        recovered_count = 0
        not_moved_count = 0
        attention_count = 0

        for operation in operations:
            claim = self._renew(claim)
            validation_error = self._validate_recovery_destination(
                operation, staging_root
            )
            source = operation.source_path
            destination = operation.destination_path
            if validation_error is not None:
                self.store.mark_move_needs_attention(
                    operation.id,
                    status_message=validation_error,
                    claim=claim,
                    update_parent=True,
                )
                attention_count += 1
                continue

            if source.is_symlink() or destination.is_symlink():
                self.store.mark_move_needs_attention(
                    operation.id,
                    status_message=(
                        "Interrupted move references a symlink; source and "
                        "destination were preserved"
                    ),
                    claim=claim,
                    update_parent=True,
                )
                attention_count += 1
                continue

            source_exists = source.exists()
            destination_exists = destination.exists()

            if source_exists and not destination_exists:
                if not self._destination_matches(
                    source,
                    operation.source_size,
                    operation.source_sha256,
                ):
                    self.store.mark_move_needs_attention(
                        operation.id,
                        status_message=(
                            "Source remains and destination is absent, but the "
                            "source fingerprint changed; preserved source for "
                            "manual inspection"
                        ),
                        claim=claim,
                        update_parent=True,
                    )
                    attention_count += 1
                    continue
                self.store.recover_move_not_moved(
                    operation.id,
                    status_message=(
                        "Move intent found but source remains and destination is "
                        "absent; move was not completed"
                    ),
                    claim=claim,
                )
                not_moved_count += 1
                continue

            if not source_exists and destination_exists:
                if not self._destination_matches(
                    destination,
                    operation.source_size,
                    operation.source_sha256,
                ):
                    self.store.mark_move_needs_attention(
                        operation.id,
                        status_message=(
                            "Source is absent and destination exists, but the "
                            "destination fingerprint does not match; preserved "
                            "destination for manual inspection"
                        ),
                        claim=claim,
                        update_parent=True,
                    )
                    attention_count += 1
                    continue
                self.store.recover_move_as_staged(
                    operation.id,
                    staged_path=destination,
                    status_message=(
                        "Recovered interrupted move after verifying destination "
                        "fingerprint"
                    ),
                    claim=claim,
                )
                recovered_count += 1
                continue

            if source_exists and destination_exists:
                message = (
                    "Both source and destination exist for interrupted move; "
                    "ambiguous; preserved both copies"
                )
            else:
                message = (
                    "Interrupted move has neither source nor destination; data "
                    "may be missing"
                )
            self.store.mark_move_needs_attention(
                operation.id,
                status_message=message,
                claim=claim,
                update_parent=True,
            )
            attention_count += 1

        return len(operations), recovered_count, not_moved_count, attention_count

    def _settle_directory_parent(
        self,
        job_id: int,
        staging_root: Path,
        claim: JobClaim,
        *,
        allow_staged: bool,
    ) -> JobClaim:
        files = self.store.list_job_files(job_id)
        if not files:
            return claim
        job = self._required_job(job_id)
        supported_files = [
            file for file in files if file.state is not JobFileState.IGNORED
        ]
        has_attention = any(
            file.state is JobFileState.NEEDS_ATTENTION for file in supported_files
        )
        if has_attention and job.state not in {
            JobState.NEEDS_ATTENTION,
            JobState.COMPLETE,
        }:
            self.store.update_job_claimed(
                job.id,
                owner_token=claim.owner_token,
                claim_fence=claim.fence,
                state=JobState.NEEDS_ATTENTION,
                status_message=(
                    "Recovery found a move requiring attention; no filesystem "
                    "copy was discarded"
                ),
            )
            return self._renew(claim)

        if (
            allow_staged
            and supported_files
            and all(file.state is JobFileState.STAGED for file in supported_files)
            and job.state in {JobState.DISCOVERED, JobState.NEEDS_ATTENTION}
        ):
            self.store.update_job_claimed(
                job.id,
                owner_token=claim.owner_token,
                claim_fence=claim.fence,
                state=JobState.STAGED,
                staged_path=staging_root,
                status_message="Recovered interrupted moves; all supported files are staged",
            )
            return self._renew(claim)
        return claim

    def _file_attention(
        self,
        file_id: int,
        message: str,
        claim: JobClaim,
    ) -> None:
        self.store.update_job_file_claimed(
            file_id,
            owner_token=claim.owner_token,
            claim_fence=claim.fence,
            state=JobFileState.NEEDS_ATTENTION,
            status_message=message,
        )

    def _attention(
        self,
        job: Job,
        message: str,
        claim: JobClaim,
    ) -> IntakeResult:
        updated = self.store.update_job_claimed(
            job.id,
            owner_token=claim.owner_token,
            claim_fence=claim.fence,
            state=JobState.NEEDS_ATTENTION,
            status_message=message,
        )
        return IntakeResult(updated, False, message)

    def _renew(self, claim: JobClaim) -> JobClaim:
        return self.store.renew_job_claim(claim)

    def _required_job(self, job_id: int) -> Job:
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError(f"Unknown job id: {job_id}")
        return job

    @staticmethod
    def _validate_recovery_destination(
        operation: MoveOperation,
        staging_root: Path,
    ) -> str | None:
        destination = operation.destination_path
        if destination.is_symlink():
            return (
                "Interrupted move destination is a symlink; source and "
                "destination were preserved"
            )
        try:
            lexical_destination = Path(os.path.abspath(destination))
            if not IntakeEngine._is_within(
                lexical_destination, Path(os.path.abspath(staging_root))
            ):
                return "Interrupted move destination escapes Staging; preserved source"
            resolved_destination = destination.resolve()
        except OSError as exc:
            return f"Could not validate interrupted move destination: {exc}"
        if not IntakeEngine._is_within(resolved_destination, staging_root):
            return "Interrupted move destination resolves outside Staging; preserved paths"
        return None

    @staticmethod
    def _fingerprint(path: Path) -> tuple[int, str]:
        if path.is_symlink() or not path.is_file():
            raise OSError(errno.EINVAL, "path is not a regular file", str(path))
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        return size, digest.hexdigest()

    @classmethod
    def _destination_matches(
        cls,
        path: Path,
        expected_size: int,
        expected_sha256: str,
    ) -> bool:
        try:
            size, sha256 = cls._fingerprint(path)
        except OSError:
            return False
        return size == expected_size and sha256 == expected_sha256

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
    def _move_error(exc: OSError, destination: Path) -> str:
        if exc.errno == errno.EXDEV:
            return (
                "Completed Torrents and Staging are on different filesystems; "
                "cross-filesystem copy-and-delete is intentionally disabled"
            )
        if exc.errno == errno.EEXIST:
            return (
                "Destination appeared during the staging move; source preserved: "
                f"{destination}"
            )
        return f"Staging move failed: {exc}"

    @staticmethod
    def _move_without_replacing(source: Path, destination: Path) -> None:
        """Atomically rename a source without replacing an existing path."""
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
