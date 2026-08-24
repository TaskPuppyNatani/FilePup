from dataclasses import dataclass
from pathlib import Path

from .database import JobStore
from .jobs import JobFile, JobFileState


VIDEO_EXTENSIONS = {
    ".mkv",
    ".mp4",
    ".avi",
    ".m4v",
    ".mov",
    ".wmv",
    ".ts",
    ".mpg",
    ".mpeg",
}

EBOOK_EXTENSIONS = {
    ".epub",
    ".pdf",
    ".mobi",
    ".azw3",
    ".cbz",
    ".cbr",
}

AUDIO_EXTENSIONS = {
    ".mp3",
    ".flac",
    ".m4a",
    ".aac",
    ".ogg",
    ".opus",
    ".wav",
    ".alac",
    ".wma",
    ".ape",
    ".aiff",
    ".aif",
    ".m4b",
}

SUPPORTED_MEDIA_EXTENSIONS = VIDEO_EXTENSIONS | EBOOK_EXTENSIONS | AUDIO_EXTENSIONS


@dataclass(frozen=True)
class InventoryResult:
    files: list[JobFile]
    supported_count: int
    ignored_count: int


class InventoryEngine:
    def __init__(self, store: JobStore):
        self.store = store

    def inventory(self, job_id: int) -> InventoryResult:
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError(f"Unknown job id: {job_id}")

        root = job.source_path.expanduser().resolve()
        if not root.exists():
            raise FileNotFoundError(root)

        if root.is_file():
            state = self._state_for(root)
            records = [(root, Path(root.name), state, self._message_for(state))]
        else:
            records = []
            for path in sorted(
                p for p in root.rglob("*") if p.is_file() and not p.is_symlink()
            ):
                resolved = path.resolve()
                state = self._state_for(path)
                records.append(
                    (
                        resolved,
                        path.relative_to(root),
                        state,
                        self._message_for(state),
                    )
                )

        files = self.store.replace_job_files(job_id, records)
        supported = sum(file.state is JobFileState.DISCOVERED for file in files)
        ignored = sum(file.state is JobFileState.IGNORED for file in files)
        return InventoryResult(files, supported, ignored)

    @staticmethod
    def _state_for(path: Path) -> JobFileState:
        if path.suffix.lower() in SUPPORTED_MEDIA_EXTENSIONS:
            return JobFileState.DISCOVERED
        return JobFileState.IGNORED

    @staticmethod
    def _message_for(state: JobFileState) -> str:
        if state is JobFileState.IGNORED:
            return "Unsupported extension; preserved in source location"
        return "Supported media file discovered"
