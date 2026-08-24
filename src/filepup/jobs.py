from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class JobState(StrEnum):
    DISCOVERED = "DISCOVERED"
    STAGED = "STAGED"
    CLASSIFIED = "CLASSIFIED"
    HANDED_OFF = "HANDED_OFF"
    WAITING_FOR_OUTPUT = "WAITING_FOR_OUTPUT"
    VERIFIED = "VERIFIED"
    SOURCE_CLEANUP = "SOURCE_CLEANUP"
    LIBRARY_REFRESH = "LIBRARY_REFRESH"
    COMPLETE = "COMPLETE"
    NEEDS_ATTENTION = "NEEDS_ATTENTION"


@dataclass(frozen=True)
class Job:
    id: int
    source_path: Path
    state: JobState
    created_at: str
    updated_at: str
    staged_path: Path | None = None
    status_message: str | None = None
