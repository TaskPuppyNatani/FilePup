import os
from dataclasses import dataclass, field
from pathlib import Path


def _path_from_env(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default))).expanduser()


@dataclass(frozen=True)
class FilePupConfig:
    database_path: Path = field(
        default_factory=lambda: _path_from_env(
            "FILEPUP_DATABASE_PATH",
            Path.home() / ".local" / "share" / "filepup" / "filepup.db",
        )
    )
    completed_torrents: Path = field(
        default_factory=lambda: _path_from_env(
            "FILEPUP_COMPLETED_TORRENTS", Path("/Media/Completed_Torrents")
        )
    )
    staging: Path = field(
        default_factory=lambda: _path_from_env("FILEPUP_STAGING", Path("/Media/Staging"))
    )
    transcoding: Path = field(
        default_factory=lambda: _path_from_env(
            "FILEPUP_TRANSCODING", Path("/Media/Transcoding")
        )
    )
    shows: Path = field(
        default_factory=lambda: _path_from_env("FILEPUP_SHOWS", Path("/Media/Shows"))
    )
    movies: Path = field(
        default_factory=lambda: _path_from_env("FILEPUP_MOVIES", Path("/Media/Movies"))
    )
    books: Path = field(
        default_factory=lambda: _path_from_env("FILEPUP_BOOKS", Path("/Media/Books"))
    )
