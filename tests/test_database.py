from pathlib import Path

from filepup.database import JobStore
from filepup.jobs import JobState


def test_ingest_is_idempotent(tmp_path: Path) -> None:
    store = JobStore(tmp_path / "filepup.db")
    source = tmp_path / "Bluey.S03E12.mkv"

    first, first_created = store.ingest(source)
    second, second_created = store.ingest(source)

    assert first_created is True
    assert second_created is False
    assert first.id == second.id
    assert first.state is JobState.DISCOVERED
