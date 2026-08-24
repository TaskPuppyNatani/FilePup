from pathlib import Path

from filepup.database import JobStore
from filepup.inventory import InventoryEngine
from filepup.jobs import JobFileState


def test_inventory_tracks_supported_and_ignored_files(tmp_path: Path) -> None:
    source = tmp_path / "Bluey Season 3"
    source.mkdir()
    (source / "Bluey.S03E12.mkv").write_bytes(b"episode-12")
    (source / "Bluey.S03E13.mp4").write_bytes(b"episode-13")
    (source / "poster.jpg").write_bytes(b"poster")
    (source / "show.nfo").write_text("metadata")

    store = JobStore(tmp_path / "filepup.db")
    job, _ = store.ingest(source)

    result = InventoryEngine(store).inventory(job.id)

    assert result.supported_count == 2
    assert result.ignored_count == 2
    assert [file.relative_path for file in result.files] == [
        Path("Bluey.S03E12.mkv"),
        Path("Bluey.S03E13.mp4"),
        Path("poster.jpg"),
        Path("show.nfo"),
    ]
    assert result.files[0].state is JobFileState.DISCOVERED
    assert result.files[1].state is JobFileState.DISCOVERED
    assert result.files[2].state is JobFileState.IGNORED
    assert result.files[3].state is JobFileState.IGNORED


def test_inventory_recognizes_audio_formats(tmp_path: Path) -> None:
    source = tmp_path / "Album"
    source.mkdir()
    audio_names = [
        "01 Track.mp3",
        "02 Track.flac",
        "03 Track.m4a",
        "04 Track.aac",
        "05 Track.ogg",
        "06 Track.opus",
        "07 Track.wav",
        "08 Track.alac",
        "09 Track.wma",
        "10 Track.ape",
        "11 Track.aiff",
        "12 Track.aif",
        "13 Audiobook.m4b",
    ]
    for name in audio_names:
        (source / name).write_bytes(b"audio")

    store = JobStore(tmp_path / "filepup.db")
    job, _ = store.ingest(source)

    result = InventoryEngine(store).inventory(job.id)

    assert result.supported_count == len(audio_names)
    assert result.ignored_count == 0
    assert all(file.state is JobFileState.DISCOVERED for file in result.files)


def test_inventory_recognizes_legacy_video_formats(tmp_path: Path) -> None:
    source = tmp_path / "Videos"
    source.mkdir()
    video_names = [
        "clip.avi",
        "clip.m4v",
        "clip.mov",
        "clip.wmv",
        "clip.ts",
        "clip.mpg",
        "clip.mpeg",
    ]
    for name in video_names:
        (source / name).write_bytes(b"video")

    store = JobStore(tmp_path / "filepup.db")
    job, _ = store.ingest(source)
    result = InventoryEngine(store).inventory(job.id)

    assert result.supported_count == len(video_names)
    assert result.ignored_count == 0


def test_inventory_ignores_symlinked_files(tmp_path: Path) -> None:
    source = tmp_path / "Torrent"
    source.mkdir()
    outside = tmp_path / "outside.mkv"
    outside.write_bytes(b"outside")
    (source / "escape.mkv").symlink_to(outside)

    store = JobStore(tmp_path / "filepup.db")
    job, _ = store.ingest(source)
    result = InventoryEngine(store).inventory(job.id)

    assert result.supported_count == 0
    assert result.ignored_count == 0
    assert result.files == []
    assert outside.read_bytes() == b"outside"


def test_inventory_preserves_nested_relative_paths(tmp_path: Path) -> None:
    source = tmp_path / "Bluey Season 3"
    nested = source / "Season 03"
    nested.mkdir(parents=True)
    episode = nested / "Bluey.S03E12.mkv"
    episode.write_bytes(b"episode")

    store = JobStore(tmp_path / "filepup.db")
    job, _ = store.ingest(source)

    result = InventoryEngine(store).inventory(job.id)

    assert result.supported_count == 1
    assert result.files[0].relative_path == Path("Season 03/Bluey.S03E12.mkv")


def test_reinventory_replaces_stale_file_records(tmp_path: Path) -> None:
    source = tmp_path / "Bluey Season 3"
    source.mkdir()
    first = source / "Bluey.S03E12.mkv"
    first.write_bytes(b"episode-12")

    store = JobStore(tmp_path / "filepup.db")
    job, _ = store.ingest(source)
    engine = InventoryEngine(store)
    engine.inventory(job.id)

    first.unlink()
    second = source / "Bluey.S03E13.mkv"
    second.write_bytes(b"episode-13")

    result = engine.inventory(job.id)

    assert [file.relative_path for file in result.files] == [Path("Bluey.S03E13.mkv")]
