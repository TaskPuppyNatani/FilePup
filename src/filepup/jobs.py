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


class JobFileState(StrEnum):
    DISCOVERED = "DISCOVERED"
    MOVING = "MOVING"
    STAGED = "STAGED"
    IGNORED = "IGNORED"
    NEEDS_ATTENTION = "NEEDS_ATTENTION"


class MoveOperationType(StrEnum):
    MOVE_TO_STAGING = "MOVE_TO_STAGING"


class MoveOperationPhase(StrEnum):
    PREPARED = "PREPARED"
    RENAMED = "RENAMED"
    COMPLETED = "COMPLETED"
    NOT_MOVED = "NOT_MOVED"
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


@dataclass(frozen=True)
class JobFile:
    id: int
    job_id: int
    source_path: Path
    relative_path: Path
    state: JobFileState
    created_at: str
    updated_at: str
    staged_path: Path | None = None
    status_message: str | None = None


@dataclass(frozen=True)
class JobClaim:
    job_id: int
    owner_token: str
    fence: int
    lease_until: float


@dataclass(frozen=True)
class MoveOperation:
    id: int
    job_id: int
    job_file_id: int | None
    operation_type: MoveOperationType
    phase: MoveOperationPhase
    source_path: Path
    destination_path: Path
    source_size: int
    source_sha256: str
    prior_file_state: JobFileState | None
    status_message: str | None
    created_at: str
    updated_at: str
