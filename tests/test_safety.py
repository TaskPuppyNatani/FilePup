from pathlib import Path

from filepup.safety import SafetyController


def test_source_deletion_is_hard_disabled() -> None:
    decision = SafetyController().may_delete_source(Path("/Media/Staging/example.mkv"))

    assert decision.allowed is False
    assert "hard-disabled" in decision.reason
