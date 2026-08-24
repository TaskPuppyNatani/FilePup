from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FilePupConfig:
    database_path: Path = Path.home() / ".local" / "share" / "filepup" / "filepup.db"
    completed_torrents: Path = Path("/Media/Completed_Torrents")
    staging: Path = Path("/Media/Staging")
    transcoding: Path = Path("/Media/Transcoding")
    shows: Path = Path("/Media/Shows")
    movies: Path = Path("/Media/Movies")
    books: Path = Path("/Media/Books")
