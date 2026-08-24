from pathlib import Path

from filepup.config import FilePupConfig
from filepup.database import JobStore
from filepup.intake import IntakeEngine
from filepup.jobs import JobState


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
