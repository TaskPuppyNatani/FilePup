import errno
import shutil
from pathlib import Path

from filepup.cli import build_parser
from filepup.config import FilePupConfig
from filepup.database import JobStore
from filepup.intake import IntakeEngine
from filepup.inventory import InventoryEngine
from filepup.jobs import JobFileState, JobState


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


def test_stage_moves_and_verifies_source(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    source.write_bytes(b"test-media")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    destination = config.staging / source.name
    assert result.moved is True
    assert result.job.state is JobState.STAGED
    assert result.job.staged_path == destination
    assert destination.read_bytes() == b"test-media"
    assert not source.exists()


def test_stage_unsupported_single_file_is_preserved(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "poster.jpg"
    source.write_bytes(b"poster")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is False
    assert result.job.state is JobState.COMPLETE
    assert source.read_bytes() == b"poster"
    assert not (config.staging / source.name).exists()


def test_stage_refuses_source_outside_completed_root(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = tmp_path / "Elsewhere" / "Bluey.S03E12.mkv"
    source.parent.mkdir()
    source.write_bytes(b"test-media")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is False
    assert result.job.state is JobState.NEEDS_ATTENTION
    assert source.exists()


def test_stage_refuses_destination_conflict(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    destination = config.staging / source.name
    source.write_bytes(b"new")
    destination.write_bytes(b"existing")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is False
    assert result.job.state is JobState.NEEDS_ATTENTION
    assert source.read_bytes() == b"new"
    assert destination.read_bytes() == b"existing"


def test_stage_verified_duplicate_is_preserved_while_deletion_disabled(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    destination = config.staging / source.name
    source.write_bytes(b"same")
    destination.write_bytes(b"same")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is False
    assert result.job.state is JobState.NEEDS_ATTENTION
    assert "Duplicate destination verified" in result.message
    assert source.read_bytes() == b"same"
    assert destination.read_bytes() == b"same"


def test_stage_refuses_missing_staging_directory(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.staging.rmdir()
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    source.write_bytes(b"test-media")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is False
    assert result.job.state is JobState.NEEDS_ATTENTION
    assert source.exists()


def test_stage_directory_moves_supported_and_preserves_ignored(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    nested = source / "Season 03"
    nested.mkdir(parents=True)
    episode = nested / "Bluey.S03E12.mkv"
    music = source / "Bluey.Theme.flac"
    poster = source / "poster.jpg"
    episode.write_bytes(b"episode")
    music.write_bytes(b"theme")
    poster.write_bytes(b"poster")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is True
    assert result.job.state is JobState.STAGED
    assert (config.staging / "Season 03" / episode.name).read_bytes() == b"episode"
    assert (config.staging / music.name).read_bytes() == b"theme"
    assert poster.read_bytes() == b"poster"
    assert not episode.exists()
    assert not music.exists()
    files = store.list_job_files(job.id)
    staged = [file for file in files if file.state is JobFileState.STAGED]
    ignored = [file for file in files if file.state is JobFileState.IGNORED]
    assert len(staged) == 2
    assert len(ignored) == 1


def test_stage_directory_conflict_allows_unrelated_safe_progress(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    first = source / "Bluey.S03E12.mkv"
    second = source / "Bluey.S03E13.mkv"
    first.write_bytes(b"episode-12")
    second.write_bytes(b"episode-13")
    (config.staging / second.name).write_bytes(b"existing")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is True
    assert result.job.state is JobState.NEEDS_ATTENTION
    assert not first.exists()
    assert (config.staging / first.name).read_bytes() == b"episode-12"
    assert second.read_bytes() == b"episode-13"
    assert (config.staging / second.name).read_bytes() == b"existing"

    files = {file.relative_path.name: file for file in store.list_job_files(job.id)}
    assert files[first.name].state is JobFileState.STAGED
    assert files[second.name].state is JobFileState.NEEDS_ATTENTION


def test_stage_directory_verified_duplicate_is_preserved_and_others_move(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    duplicate = source / "Bluey.S03E12.mkv"
    movable = source / "Bluey.S03E13.mkv"
    duplicate.write_bytes(b"same")
    movable.write_bytes(b"episode-13")
    (config.staging / duplicate.name).write_bytes(b"same")
    job, _ = store.ingest(source)

    result = IntakeEngine(config, store).stage(job.id)

    assert result.moved is True
    assert result.job.state is JobState.NEEDS_ATTENTION
    assert duplicate.read_bytes() == b"same"
    assert (config.staging / duplicate.name).read_bytes() == b"same"
    assert not movable.exists()
    assert (config.staging / movable.name).read_bytes() == b"episode-13"

    files = {file.relative_path.name: file for file in store.list_job_files(job.id)}
    assert files[duplicate.name].state is JobFileState.NEEDS_ATTENTION
    assert files[movable.name].state is JobFileState.STAGED


def test_reinventory_after_partial_stage_preserves_history_without_duplicate_rows(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    staged_source = source / "Bluey.S03E12.mkv"
    attention_source = source / "Bluey.S03E13.mkv"
    ignored_source = source / "poster.jpg"
    staged_source.write_bytes(b"episode-12")
    attention_source.write_bytes(b"episode-13")
    ignored_source.write_bytes(b"poster")
    (config.staging / attention_source.name).write_bytes(b"existing")
    job, _ = store.ingest(source)

    engine = IntakeEngine(config, store)
    engine.stage(job.id)
    before = {file.relative_path.name: file for file in store.list_job_files(job.id)}

    result = InventoryEngine(store).inventory(job.id)
    after = {file.relative_path.name: file for file in result.files}

    assert len(result.files) == 3
    assert len({file.id for file in result.files}) == 3
    assert after[staged_source.name].id == before[staged_source.name].id
    assert after[staged_source.name].state is JobFileState.STAGED
    assert after[staged_source.name].staged_path == config.staging / staged_source.name
    assert after[attention_source.name].id == before[attention_source.name].id
    assert after[attention_source.name].state is JobFileState.NEEDS_ATTENTION
    assert after[ignored_source.name].state is JobFileState.IGNORED
    assert after[staged_source.name].updated_at == before[staged_source.name].updated_at
    assert after[attention_source.name].updated_at == before[attention_source.name].updated_at


def test_retry_resolves_conflict_without_moving_staged_child_and_is_idempotent(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    staged_source = source / "Bluey.S03E12.mkv"
    attention_source = source / "Bluey.S03E13.mkv"
    staged_source.write_bytes(b"episode-12")
    attention_source.write_bytes(b"episode-13")
    conflict = config.staging / attention_source.name
    conflict.write_bytes(b"existing")
    job, _ = store.ingest(source)

    engine = IntakeEngine(config, store)
    initial = engine.stage(job.id)
    assert initial.job.state is JobState.NEEDS_ATTENTION
    staged_destination = config.staging / staged_source.name
    staged_stat = staged_destination.stat()
    staged_before = next(
        file
        for file in store.list_job_files(job.id)
        if file.relative_path.name == staged_source.name
    )

    conflict.unlink()
    retried = engine.retry(job.id)

    assert retried.moved is True
    assert retried.job.state is JobState.STAGED
    assert not attention_source.exists()
    assert (config.staging / attention_source.name).read_bytes() == b"episode-13"
    assert staged_destination.stat().st_ino == staged_stat.st_ino
    assert staged_destination.stat().st_mtime_ns == staged_stat.st_mtime_ns
    staged_after = next(
        file
        for file in store.list_job_files(job.id)
        if file.relative_path.name == staged_source.name
    )
    assert staged_after.id == staged_before.id
    assert staged_after.state is JobFileState.STAGED
    assert staged_after.updated_at == staged_before.updated_at

    first_retry_snapshot = {
        file.relative_path.name: (file.id, file.state, file.staged_path, file.updated_at)
        for file in store.list_job_files(job.id)
    }
    repeated = engine.retry(job.id)
    second_retry_snapshot = {
        file.relative_path.name: (file.id, file.state, file.staged_path, file.updated_at)
        for file in store.list_job_files(job.id)
    }

    assert repeated.moved is False
    assert repeated.job.state is JobState.STAGED
    assert "not awaiting retry" in repeated.message
    assert first_retry_snapshot == second_retry_snapshot
    assert len(store.list_job_files(job.id)) == 2


def test_retry_with_conflict_remaining_preserves_all_files_and_history(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    staged_source = source / "Bluey.S03E12.mkv"
    attention_source = source / "Bluey.S03E13.mkv"
    staged_source.write_bytes(b"episode-12")
    attention_source.write_bytes(b"episode-13")
    conflict = config.staging / attention_source.name
    conflict.write_bytes(b"existing")
    job, _ = store.ingest(source)

    engine = IntakeEngine(config, store)
    engine.stage(job.id)
    staged_destination = config.staging / staged_source.name
    staged_stat = staged_destination.stat()
    before_ids = {file.relative_path.name: file.id for file in store.list_job_files(job.id)}
    source_bytes = attention_source.read_bytes()
    destination_bytes = conflict.read_bytes()

    retried = engine.retry(job.id)

    assert retried.moved is False
    assert retried.job.state is JobState.NEEDS_ATTENTION
    assert attention_source.read_bytes() == source_bytes
    assert conflict.read_bytes() == destination_bytes
    assert staged_destination.stat().st_ino == staged_stat.st_ino
    assert before_ids == {
        file.relative_path.name: file.id for file in store.list_job_files(job.id)
    }
    assert "1 file(s) need attention" in retried.message


def test_retry_duplicate_keeps_both_copies_when_deletion_is_disabled(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    staged_source = source / "Bluey.S03E12.mkv"
    attention_source = source / "Bluey.S03E13.mkv"
    staged_source.write_bytes(b"episode-12")
    attention_source.write_bytes(b"episode-13")
    conflict = config.staging / attention_source.name
    conflict.write_bytes(b"different")
    job, _ = store.ingest(source)

    engine = IntakeEngine(config, store)
    engine.stage(job.id)
    conflict.write_bytes(attention_source.read_bytes())

    retried = engine.retry(job.id)

    assert retried.job.state is JobState.NEEDS_ATTENTION
    assert retried.moved is False
    assert "verified duplicate" in retried.message
    assert "hard-disabled" in retried.message
    assert attention_source.read_bytes() == b"episode-13"
    assert conflict.read_bytes() == b"episode-13"
    attention = next(
        file
        for file in store.list_job_files(job.id)
        if file.relative_path.name == attention_source.name
    )
    assert attention.state is JobFileState.NEEDS_ATTENTION


def test_retry_missing_staging_does_not_mutate_source_or_staged_history(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    staged_source = source / "Bluey.S03E12.mkv"
    attention_source = source / "Bluey.S03E13.mkv"
    staged_source.write_bytes(b"episode-12")
    attention_source.write_bytes(b"episode-13")
    (config.staging / attention_source.name).write_bytes(b"existing")
    job, _ = store.ingest(source)

    engine = IntakeEngine(config, store)
    engine.stage(job.id)
    before = {
        file.relative_path.name: (file.id, file.state, file.updated_at)
        for file in store.list_job_files(job.id)
    }
    source_bytes = attention_source.read_bytes()
    shutil.rmtree(config.staging)

    retried = engine.retry(job.id)

    assert retried.job.state is JobState.NEEDS_ATTENTION
    assert retried.moved is False
    assert "Staging directory is unavailable" in retried.message
    assert attention_source.read_bytes() == source_bytes
    assert {
        file.relative_path.name: (file.id, file.state, file.updated_at)
        for file in store.list_job_files(job.id)
    } == before


def test_retry_refuses_source_escape_and_symlink_substitution(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    staged_source = source / "Bluey.S03E12.mkv"
    attention_source = source / "Bluey.S03E13.mkv"
    outside = tmp_path / "outside.mkv"
    staged_source.write_bytes(b"episode-12")
    attention_source.write_bytes(b"episode-13")
    outside.write_bytes(b"outside")
    (config.staging / attention_source.name).write_bytes(b"existing")
    job, _ = store.ingest(source)

    engine = IntakeEngine(config, store)
    engine.stage(job.id)
    attention_source.unlink()
    attention_source.symlink_to(outside)

    retried = engine.retry(job.id)

    assert retried.job.state is JobState.NEEDS_ATTENTION
    assert retried.moved is False
    assert attention_source.is_symlink()
    assert outside.read_bytes() == b"outside"
    attention = next(
        file
        for file in store.list_job_files(job.id)
        if file.relative_path.name == attention_source.name
    )
    assert "symlink" in (attention.status_message or "")


def test_retry_reconsiders_only_unresolved_children_when_some_still_fail(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.Season.03"
    source.mkdir()
    first = source / "Bluey.S03E12.mkv"
    resolved = source / "Bluey.S03E13.mkv"
    still_blocked = source / "Bluey.S03E14.mkv"
    first.write_bytes(b"episode-12")
    resolved.write_bytes(b"episode-13")
    still_blocked.write_bytes(b"episode-14")
    resolved_destination = config.staging / resolved.name
    blocked_destination = config.staging / still_blocked.name
    resolved_destination.write_bytes(b"existing")
    blocked_destination.write_bytes(b"existing")
    job, _ = store.ingest(source)

    engine = IntakeEngine(config, store)
    engine.stage(job.id)
    resolved_destination.unlink()
    retried = engine.retry(job.id)

    assert retried.job.state is JobState.NEEDS_ATTENTION
    assert retried.moved is True
    assert not resolved.exists()
    assert resolved_destination.read_bytes() == b"episode-13"
    assert still_blocked.read_bytes() == b"episode-14"
    assert blocked_destination.read_bytes() == b"existing"
    assert "Staged 1 supported media file(s)" in retried.message
    assert "1 file(s) need attention" in retried.message
    files = {file.relative_path.name: file for file in store.list_job_files(job.id)}
    assert files[first.name].state is JobFileState.STAGED
    assert files[resolved.name].state is JobFileState.STAGED
    assert files[still_blocked.name].state is JobFileState.NEEDS_ATTENTION


def test_retry_single_file_after_conflict_resolution(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    destination = config.staging / source.name
    source.write_bytes(b"episode-12")
    destination.write_bytes(b"existing")
    job, _ = store.ingest(source)
    engine = IntakeEngine(config, store)

    initial = engine.stage(job.id)
    destination.unlink()
    retried = engine.retry(job.id)

    assert initial.job.state is JobState.NEEDS_ATTENTION
    assert retried.job.state is JobState.STAGED
    assert retried.moved is True
    assert not source.exists()
    assert destination.read_bytes() == b"episode-12"


def test_stage_does_not_retry_attention_jobs_implicitly(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = JobStore(config.database_path)
    source = config.completed_torrents / "Bluey.S03E12.mkv"
    destination = config.staging / source.name
    source.write_bytes(b"episode-12")
    destination.write_bytes(b"existing")
    job, _ = store.ingest(source)
    engine = IntakeEngine(config, store)

    engine.stage(job.id)
    destination.unlink()
    result = engine.stage(job.id)

    assert result.job.state is JobState.NEEDS_ATTENTION
    assert result.moved is False
    assert source.exists()
    assert not destination.exists()


def test_no_replace_move_never_overwrites_existing_destination(tmp_path: Path) -> None:
    source = tmp_path / "source.mkv"
    destination = tmp_path / "destination.mkv"
    source.write_bytes(b"source")
    destination.write_bytes(b"destination")

    try:
        IntakeEngine._move_without_replacing(source, destination)
    except OSError as exc:
        assert exc.errno == errno.EEXIST
    else:
        raise AssertionError("no-replace move unexpectedly replaced destination")

    assert source.read_bytes() == b"source"
    assert destination.read_bytes() == b"destination"


def test_cli_exposes_explicit_retry_command() -> None:
    args = build_parser().parse_args(["retry", "42"])

    assert args.command == "retry"
    assert args.job_id == 42
