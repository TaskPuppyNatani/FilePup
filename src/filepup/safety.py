from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DeletionDecision:
    allowed: bool
    reason: str


class SafetyController:
    """Central authority for destructive file operations.

    Deletion is intentionally disabled in the initial scaffold. No other
    FilePup module should delete source media directly.
    """

    def may_delete_source(self, source_path: Path) -> DeletionDecision:
        return DeletionDecision(
            allowed=False,
            reason=(
                "Source deletion is hard-disabled in FilePup v0.0.1. "
                f"Preserving {Path(source_path)}."
            ),
        )
