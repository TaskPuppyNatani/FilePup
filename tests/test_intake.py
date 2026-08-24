from pathlib import Path

from filepup.config import FilePupConfig
from filepup.database import JobStore
from filepup.intake import IntakeEngine
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
