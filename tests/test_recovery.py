import multiprocessing
import time
from pathlib import Path

import pytest

from filepup.config import FilePupConfig
from filepup.database import JobClaimError, JobStore
from filepup.intake import IntakeEngine
from filepup.inventory import InventoryEngine
from filepup.jobs import (
    JobFileState,
    JobState,
    MoveOperationPhase,
    MoveOperationType,
)


def hold_job_lock(
    database_path: str,
    job_id: int,
    ready,
    release,
) -> None:
    store = JobStore(Path(database_path))
    with store.job_lock(job_id):
        ready.set()
        release.wait(5)


def make_config(tmp_path: Path) -> FilePupConfig:
    completed = tmp_path / "Completed_Torrents"
    staging = tmp_path / "Staging"
    completed.mkdir()
    staging.mkdir()
    return FilePupConfig(
        database_path=tmp_path / "filepup.db",
        completed_torrents=completed,
        staging=staging,
        transcoding=tmp_path / "Transcoding",
        shows=tmp_path / "Shows",
        movies=tmp_path / "Movies",
        books=tmp_path / "Books",
    )


def prepare_directory_job(tmp_path: Path):
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source_root = config.completed_torrents / "Bluey.Season.03"
    source_root.mkdir()
    source = source_root / "Bluey.S03E12.mkv"
    source.write_bytes(b"episode-12")
    job, _ = store.ingest(source_root)
    records = InventoryEngine(store).inventory(job.id).files
    record = next(file for file in records if file.state is JobFileState.DISCOVERED)
    return config, store, job, source, record


def prepare_operation(
    store: JobStore,
    job_id: int,
    source: Path,
    destination: Path,
    *,
    file_id: int | None = None,
    prior_state: JobFileState | None = None,
):
    claim = store.claim_job(job_id, "crashed-worker")
    assert claim is not None
    size, sha256 = IntakeEngine._fingerprint(source)
    operation = store.begin_move_operation(
        job_id=job_id,
        job_file_id=file_id,
        operation_type=MoveOperationType.MOVE_TO_STAGING,
        source_path=source,
        destination_path=destination,
        source_size=size,
        source_sha256=sha256,
        prior_file_state=prior_state,
        claim=claim,
    )
    store.release_job_claim(claim)
    return operation


def mark_operation_renamed(store: JobStore, operation_id: int, job_id: int) -> None:
    claim = store.claim_job(job_id, "rename-recording-worker")
    assert claim is not None
    store.mark_move_renamed(operation_id, claim)
    store.release_job_claim(claim)


def test_prepared_recovery_defers_when_staging_root_is_unavailable(
    tmp_path: Path,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    operation = prepare_operation(
        store,
        job.id,
        source,
        config.staging / source.name,
        file_id=record.id,
        prior_state=record.state,
    )
    before_job = store.get_job(job.id)
    before_file = store.list_job_files(job.id)[0]
    config.staging.rmdir()

    result = IntakeEngine(config, store).recover(job.id)

    assert result.moved is False
    assert "Recovery deferred" in result.message
    assert "Staging" in result.message
    assert result.job == before_job
    assert not config.staging.exists()
    assert source.exists()
    assert not (config.staging / source.name).exists()
    assert store.get_move_operation(operation.id).phase is MoveOperationPhase.PREPARED
    assert store.list_job_files(job.id)[0] == before_file


def test_prepared_recovery_defers_when_completed_root_is_unavailable(
    tmp_path: Path,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    operation = prepare_operation(
        store,
        job.id,
        source,
        config.staging / source.name,
        file_id=record.id,
        prior_state=record.state,
    )
    hidden_completed = tmp_path / "Completed_Torrents.hidden"
    config.completed_torrents.rename(hidden_completed)
    hidden_source = hidden_completed / source.relative_to(config.completed_torrents)

    try:
        result = IntakeEngine(config, store).recover(job.id)

        assert result.moved is False
        assert "Recovery deferred" in result.message
        assert "Completed Torrents" in result.message
        assert not config.completed_torrents.exists()
        assert hidden_source.exists()
        assert store.get_move_operation(operation.id).phase is MoveOperationPhase.PREPARED
        assert store.list_job_files(job.id)[0].state is JobFileState.MOVING
    finally:
        hidden_completed.rename(config.completed_torrents)


def test_renamed_recovery_defers_without_staging_then_reconciles_after_restore(
    tmp_path: Path,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    destination = config.staging / source.name
    operation = prepare_operation(
        store,
        job.id,
        source,
        destination,
        file_id=record.id,
        prior_state=record.state,
    )
    IntakeEngine._move_without_replacing(source, destination)
    mark_operation_renamed(store, operation.id, job.id)
    hidden_staging = tmp_path / "Staging.hidden"
    config.staging.rename(hidden_staging)

    try:
        deferred = IntakeEngine(config, store).recover(job.id)

        assert deferred.moved is False
        assert "Recovery deferred" in deferred.message
        assert not config.staging.exists()
        assert not source.exists()
        assert (hidden_staging / destination.relative_to(config.staging)).exists()
        assert store.get_move_operation(operation.id).phase is MoveOperationPhase.RENAMED
        assert store.list_job_files(job.id)[0].state is JobFileState.MOVING
    finally:
        hidden_staging.rename(config.staging)

    recovered = IntakeEngine(config, store).recover(job.id)

    assert recovered.moved is True
    assert recovered.job.state is JobState.STAGED
    assert destination.read_bytes() == b"episode-12"
    assert store.get_move_operation(operation.id).phase is MoveOperationPhase.COMPLETED
    assert store.list_job_files(job.id)[0].state is JobFileState.STAGED


def test_repeated_deferred_recovery_is_idempotent_and_creates_no_media_root(
    tmp_path: Path,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    operation = prepare_operation(
        store,
        job.id,
        source,
        config.staging / source.name,
        file_id=record.id,
        prior_state=record.state,
    )
    config.staging.rmdir()
    before = (
        store.get_job(job.id),
        store.list_job_files(job.id)[0],
        store.get_move_operation(operation.id),
    )

    first = IntakeEngine(config, store).recover(job.id)
    second = IntakeEngine(config, store).recover(job.id)
    after = (
        store.get_job(job.id),
        store.list_job_files(job.id)[0],
        store.get_move_operation(operation.id),
    )

    assert first.moved is False
    assert second.moved is False
    assert "Recovery deferred" in first.message
    assert "Recovery deferred" in second.message
    assert before == after
    assert not config.staging.exists()
    assert source.exists()


@pytest.mark.parametrize("missing_root", ["completed", "staging"])
def test_retry_with_pending_move_also_defers_when_media_root_is_unavailable(
    tmp_path: Path,
    missing_root: str,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    operation = prepare_operation(
        store,
        job.id,
        source,
        config.staging / source.name,
        file_id=record.id,
        prior_state=record.state,
    )
    store.update_job(job.id, state=JobState.NEEDS_ATTENTION)
    hidden_root = tmp_path / f"{missing_root}.hidden"
    if missing_root == "completed":
        config.completed_torrents.rename(hidden_root)
    else:
        config.staging.rmdir()
    before = (
        store.get_job(job.id),
        store.list_job_files(job.id)[0],
        store.get_move_operation(operation.id),
    )

    try:
        result = IntakeEngine(config, store).retry(job.id)

        assert result.moved is False
        assert "Recovery deferred" in result.message
        assert (
            store.get_job(job.id),
            store.list_job_files(job.id)[0],
            store.get_move_operation(operation.id),
        ) == before
    finally:
        if missing_root == "completed":
            hidden_root.rename(config.completed_torrents)
        else:
            config.staging.mkdir()


def test_recovery_before_rename_records_not_moved_without_false_staged(
    tmp_path: Path,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    destination = config.staging / source.name
    operation = prepare_operation(
        store,
        job.id,
        source,
        destination,
        file_id=record.id,
        prior_state=record.state,
    )

    result = IntakeEngine(config, store).recover(job.id)

    assert result.moved is False
    assert result.job.state is JobState.DISCOVERED
    assert source.read_bytes() == b"episode-12"
    assert not destination.exists()
    recovered_file = store.list_job_files(job.id)[0]
    assert recovered_file.id == record.id
    assert recovered_file.state is JobFileState.DISCOVERED
    assert store.get_move_operation(operation.id).phase is MoveOperationPhase.NOT_MOVED


def test_recovery_after_rename_before_database_update_commits_staged_atomically(
    tmp_path: Path,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    destination = config.staging / source.name
    operation = prepare_operation(
        store,
        job.id,
        source,
        destination,
        file_id=record.id,
        prior_state=record.state,
    )
    IntakeEngine._move_without_replacing(source, destination)

    first = IntakeEngine(config, store).recover(job.id)
    before_repeat = store.list_job_files(job.id)[0]
    operation_after = store.get_move_operation(operation.id)

    assert first.moved is True
    assert first.job.state is JobState.STAGED
    assert not source.exists()
    assert destination.read_bytes() == b"episode-12"
    assert before_repeat.state is JobFileState.STAGED
    assert before_repeat.staged_path == destination
    assert operation_after.phase is MoveOperationPhase.COMPLETED

    repeated = IntakeEngine(config, store).recover(job.id)
    after_repeat = store.list_job_files(job.id)[0]

    assert repeated.moved is False
    assert "no pending move operations" in repeated.message
    assert after_repeat == before_repeat
    assert store.get_move_operation(operation.id) == operation_after


def test_single_file_recovery_after_rename_marks_parent_staged(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    destination = config.staging / source.name
    source.write_bytes(b"episode-12")
    job, _ = store.ingest(source)
    operation = prepare_operation(store, job.id, source, destination)
    IntakeEngine._move_without_replacing(source, destination)

    result = IntakeEngine(config, store).recover(job.id)

    assert result.job.state is JobState.STAGED
    assert result.job.staged_path == destination
    assert store.get_move_operation(operation.id).phase is MoveOperationPhase.COMPLETED


@pytest.mark.parametrize("filesystem_state", ["both", "neither"])
def test_recovery_marks_ambiguous_or_missing_data_for_attention(
    tmp_path: Path,
    filesystem_state: str,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    destination = config.staging / source.name
    operation = prepare_operation(
        store,
        job.id,
        source,
        destination,
        file_id=record.id,
        prior_state=record.state,
    )
    if filesystem_state == "both":
        destination.write_bytes(source.read_bytes())
    else:
        source.unlink()

    result = IntakeEngine(config, store).recover(job.id)

    assert result.job.state is JobState.NEEDS_ATTENTION
    assert store.list_job_files(job.id)[0].state is JobFileState.NEEDS_ATTENTION
    assert store.get_move_operation(operation.id).phase is MoveOperationPhase.NEEDS_ATTENTION
    if filesystem_state == "both":
        assert source.read_bytes() == destination.read_bytes() == b"episode-12"
        assert "Both source and destination" in result.message
    else:
        assert not source.exists()
        assert not destination.exists()
        assert "neither source nor destination" in result.message


def test_reinventory_preserves_interrupted_moving_row_and_audit_identity(
    tmp_path: Path,
) -> None:
    config, store, job, source, record = prepare_directory_job(tmp_path)
    destination = config.staging / source.name
    operation = prepare_operation(
        store,
        job.id,
        source,
        destination,
        file_id=record.id,
        prior_state=record.state,
    )
    before = store.list_job_files(job.id)[0]

    result = InventoryEngine(store).inventory(job.id)
    after = result.files[0]

    assert after.id == before.id == record.id
    assert after.state is JobFileState.MOVING
    assert after.created_at == before.created_at
    assert store.get_move_operation(operation.id).phase is MoveOperationPhase.PREPARED


def test_recovery_does_not_touch_previously_staged_child_history(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source_root = config.completed_torrents / "Bluey.Season.03"
    source_root.mkdir()
    first_source = source_root / "Bluey.S03E12.mkv"
    second_source = source_root / "Bluey.S03E13.mkv"
    first_source.write_bytes(b"episode-12")
    second_source.write_bytes(b"episode-13")
    job, _ = store.ingest(source_root)
    records = InventoryEngine(store).inventory(job.id).files
    first_record = next(file for file in records if file.source_path == first_source)
    second_record = next(file for file in records if file.source_path == second_source)
    first_destination = config.staging / first_source.name
    IntakeEngine._move_without_replacing(first_source, first_destination)
    store.update_job_file(
        first_record.id,
        state=JobFileState.STAGED,
        staged_path=first_destination,
        status_message="Moved to Staging and verified",
    )
    before = store.list_job_files(job.id)
    before_first = next(file for file in before if file.id == first_record.id)
    destination = config.staging / second_source.name
    operation = prepare_operation(
        store,
        job.id,
        second_source,
        destination,
        file_id=second_record.id,
        prior_state=second_record.state,
    )
    IntakeEngine._move_without_replacing(second_source, destination)

    result = IntakeEngine(config, store).recover(job.id)

    after_first = next(
        file for file in store.list_job_files(job.id) if file.id == first_record.id
    )
    assert result.job.state is JobState.STAGED
    assert after_first == before_first
    assert store.get_move_operation(operation.id).phase is MoveOperationPhase.COMPLETED


def test_two_workers_cannot_claim_one_job_and_different_jobs_are_independent(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    first_source = config.completed_torrents / "first.mkv"
    second_source = config.completed_torrents / "second.mkv"
    first_source.write_bytes(b"first")
    second_source.write_bytes(b"second")
    first_job, _ = store.ingest(first_source)
    second_job, _ = store.ingest(second_source)
    first_claim = store.claim_job(first_job.id, "worker-a")
    assert first_claim is not None

    assert store.claim_job(first_job.id, "worker-b") is None
    blocked = IntakeEngine(config, store).stage(first_job.id)
    independent = IntakeEngine(config, store).stage(second_job.id)

    assert blocked.moved is False
    assert "already being processed" in blocked.message
    assert first_source.exists()
    assert independent.moved is True
    assert not second_source.exists()
    store.release_job_claim(first_claim)


def test_job_lock_is_cross_process_and_second_worker_does_no_io(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    source.write_bytes(b"episode-12")
    job, _ = store.ingest(source)
    context = multiprocessing.get_context("fork")
    ready = context.Event()
    release = context.Event()
    worker = context.Process(
        target=hold_job_lock,
        args=(str(config.database_path), job.id, ready, release),
    )
    worker.start()
    try:
        assert ready.wait(5)
        result = IntakeEngine(config, store).stage(job.id)
        assert result.moved is False
        assert "already being processed" in result.message
        assert source.exists()
        assert not (config.staging / source.name).exists()
    finally:
        release.set()
        worker.join(5)
    assert worker.exitcode == 0


def test_stale_claim_can_be_reclaimed_and_old_fence_cannot_write(tmp_path: Path) -> None:
    store = JobStore(tmp_path / "filepup.db")
    source = tmp_path / "source.mkv"
    job, _ = store.ingest(source)
    old_claim = store.claim_job(
        job.id,
        "old-worker",
        lease_seconds=1,
        now=time.time() - 10,
    )
    assert old_claim is not None
    new_claim = store.claim_job(job.id, "new-worker")
    assert new_claim is not None
    assert new_claim.fence == old_claim.fence + 1

    with pytest.raises(JobClaimError):
        store.update_job_claimed(
            job.id,
            owner_token=old_claim.owner_token,
            claim_fence=old_claim.fence,
            state=JobState.NEEDS_ATTENTION,
            status_message="stale worker must not write",
        )

    store.release_job_claim(new_claim)
